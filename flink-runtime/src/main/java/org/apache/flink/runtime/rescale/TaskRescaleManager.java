/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rescale;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.SubpartitionIndexRange;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.runtime.shuffle.ShuffleUtils.applyWithShuffleTypeCheck;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class TaskRescaleManager {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRescaleManager.class);

    private final JobID jobId;

    private final ExecutionAttemptID executionId;

    private final String taskNameWithSubtaskAndId;

    private final TaskActions taskActions;

    private final NettyShuffleEnvironment shuffleEnvironment;

    private final IOManager ioManager;

    private final TaskMetricGroup metrics;

    private final TaskEventDispatcher taskEventDispatcher;

    private final ShuffleIOOwnerContext taskShuffleContext;

    private volatile TaskRescaleMeta rescaleMeta;

    private volatile ResultPartitionWriter[] storedOldWriterCopies;

    public TaskRescaleManager(
            JobID jobId,
            ExecutionAttemptID executionId,
            String taskNameWithSubtaskAndId,
            TaskActions taskActions,
            NettyShuffleEnvironment shuffleEnvironment,
            TaskEventDispatcher taskEventDispatcher,
            IOManager ioManager,
            TaskMetricGroup metrics,
            ShuffleIOOwnerContext taskShuffleContext) {

        this.jobId = checkNotNull(jobId);
        this.executionId = checkNotNull(executionId);
        this.taskNameWithSubtaskAndId = checkNotNull(taskNameWithSubtaskAndId);
        this.taskActions = checkNotNull(taskActions);
        this.shuffleEnvironment = checkNotNull(shuffleEnvironment);
        this.taskEventDispatcher = checkNotNull(taskEventDispatcher);
        this.ioManager = checkNotNull(ioManager);
        this.metrics = checkNotNull(metrics);
        this.taskShuffleContext = checkNotNull(taskShuffleContext);
    }

    public void createNewResultPartitions() throws IOException {
        // TODO: metrics will be created which should be reusable, need to understand and update the
        // logic.

        // produced intermediate result partitions
        final ResultPartitionWriter[] newResultPartitionWriters =
                shuffleEnvironment
                        .createResultPartitionWriters(
                                taskShuffleContext,
                                new ArrayList<>(
                                        rescaleMeta.getResultPartitionDeploymentDescriptors()))
                        .toArray(new ResultPartitionWriter[] {});

        // TODO: This is a test version of Trisk 1.16.
        //        ResultPartitionWriter[] newPartitions =
        // ConsumableNotifyingResultPartitionWriterDecorator.decorate(
        //                rescaleMeta.getResultPartitionDeploymentDescriptors(),
        //                newResultPartitionWriters,
        //                taskActions,
        //                jobId,
        //                resultPartitionConsumableNotifier);

        // setup partition, get bufferpool
        int index = 0;
        for (ResultPartitionWriter newPartition : newResultPartitionWriters) {
            newPartition.setup(); // Here you get a buffer pool.
            rescaleMeta.addNewPartitions(index, newPartition);
            ++index;
        }

        for (ResultPartitionWriter partitionWriter : newResultPartitionWriters) {
            taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
        }
    }

    public boolean isScalingTarget() {
        return rescaleMeta != null;
    }

    public boolean isScalingPartitions() {
        return rescaleMeta.getRescaleOptions().isScalingPartitions();
    }

    public boolean isScalingGates() {
        return rescaleMeta.getRescaleOptions().isScalingGates();
    }

    public void substituteInputGateChannels(SingleInputGate inputGate)
            throws IOException, InterruptedException {
        checkNotNull(rescaleMeta, "rescale component cannot be null");

        // only need to update input channels inside, no need to close and restart the input gate.
        InputGateDeploymentDescriptor igdd = rescaleMeta.getMatchedInputGateDescriptor(inputGate);
        ShuffleDescriptor[] shuffleDescriptors = checkNotNull(igdd.getShuffleDescriptors());

        for (ShuffleDescriptor shuffleDescriptor : shuffleDescriptors) {
            for (InputChannel channel : inputGate.getInputChannels().values()) {
                if (channel.getPartitionId().equals(shuffleDescriptor.getResultPartitionID())) {
                    // we should not request same partition twice
                    return;
                }
            }
        }

        inputGate.reset(
                calculateNumChannels(
                        shuffleDescriptors.length, inputGate.getSubpartitionIndexRange()));

        createChannels(inputGate, shuffleDescriptors);
        // Not needed for Flink 1.16.
        //        inputGate.assignExclusiveSegments();
        inputGate.setupChannels();
        inputGate.requestPartitions();
    }

    private void createChannels(SingleInputGate inputGate, ShuffleDescriptor[] shuffleDescriptors) {
        @SuppressWarnings("deprecation")
        InputChannelMetrics inputChannelMetrics =
                new InputChannelMetrics(
                        taskShuffleContext.getInputGroup(), taskShuffleContext.getParentGroup());

        // Create the input channels. There is one input channel for each consumed subpartition.
        InputChannel[] inputChannels =
                new InputChannel
                        [calculateNumChannels(
                                shuffleDescriptors.length, inputGate.getSubpartitionIndexRange())];

        int channelIdx = 0;
        for (int i = 0; i < shuffleDescriptors.length; i++) {
            for (int subpartitionIndex = inputGate.getSubpartitionIndexRange().getStartIndex();
                    subpartitionIndex <= inputGate.getSubpartitionIndexRange().getEndIndex();
                    ++subpartitionIndex) {
                InputChannel inputChannel =
                        createChannel(
                                inputGate,
                                channelIdx,
                                shuffleDescriptors[i],
                                subpartitionIndex,
                                inputChannelMetrics);
                inputChannels[channelIdx] = inputChannel;
                channelIdx++;
            }
        }
        inputGate.setInputChannels(inputChannels);
    }

    private InputChannel createChannel(
            SingleInputGate inputGate,
            int index,
            ShuffleDescriptor shuffleDescriptor,
            int consumedSubpartitionIndex,
            InputChannelMetrics metrics) {
        return applyWithShuffleTypeCheck(
                NettyShuffleDescriptor.class,
                shuffleDescriptor,
                unknownShuffleDescriptor -> {
                    throw new IllegalArgumentException(
                            "unknownShuffleDescriptor is not suppported now.");
                },
                nettyShuffleDescriptor ->
                        createKnownInputChannel(
                                inputGate,
                                index,
                                nettyShuffleDescriptor,
                                consumedSubpartitionIndex,
                                metrics));
    }

    private static int calculateNumChannels(
            int numShuffleDescriptors, SubpartitionIndexRange subpartitionIndexRange) {
        return MathUtils.checkedDownCast(
                ((long) numShuffleDescriptors) * subpartitionIndexRange.size());
    }

    private InputChannel createKnownInputChannel(
            SingleInputGate inputGate,
            int index,
            NettyShuffleDescriptor inputChannelDescriptor,
            int consumedSubpartitionIndex,
            InputChannelMetrics metrics) {
        ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
        if (inputChannelDescriptor.isLocalTo(shuffleEnvironment.getTaskExecutorResourceId())) {
            // Consuming task is deployed to the same TaskManager as the partition => local
            // Unaligned Checkpoint not supported for now.
            // To update to unaligned checkpoint, need to change ChannelStateWriter to non NO_OP.
            return new LocalInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    consumedSubpartitionIndex,
                    shuffleEnvironment.getResultPartitionManager(),
                    taskEventDispatcher,
                    shuffleEnvironment.getConfiguration().partitionRequestInitialBackoff(),
                    shuffleEnvironment.getConfiguration().partitionRequestMaxBackoff(),
                    metrics.getNumBytesInLocalCounter(),
                    metrics.getNumBuffersInLocalCounter(),
                    ChannelStateWriter.NO_OP);
        } else {
            // Different instances => remote
            // Unaligned Checkpoint not supported for now.
            // To update to unaligned checkpoint, need to change ChannelStateWriter to non NO_OP.
            return new RemoteInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    consumedSubpartitionIndex,
                    inputChannelDescriptor.getConnectionId(),
                    shuffleEnvironment.getConnectionManager(),
                    shuffleEnvironment.getConfiguration().partitionRequestInitialBackoff(),
                    shuffleEnvironment.getConfiguration().partitionRequestMaxBackoff(),
                    shuffleEnvironment.getConfiguration().networkBuffersPerChannel(),
                    metrics.getNumBytesInRemoteCounter(),
                    metrics.getNumBuffersInRemoteCounter(),
                    ChannelStateWriter.NO_OP);
        }
    }

    public ResultPartitionWriter[] substituteResultPartitions(ResultPartitionWriter[] oldWriters) {
        ResultPartitionWriter[] oldWriterCopies = Arrays.copyOf(oldWriters, oldWriters.length);

        for (int i = 0; i < oldWriters.length; i++) {
            oldWriters[i] = rescaleMeta.getNewPartitions(i);
        }

        return oldWriterCopies;
    }

    // We cannot do it immediately because downstream's gate is still polling from the old
    // partitions (barrier haven't pass to downstream)
    // so we store the oldWriterCopies and unregister them in next scaling.
    public void unregisterPartitions(ResultPartitionWriter[] oldWriterCopies) {
        if (storedOldWriterCopies != null) {
            shuffleEnvironment.unregisterPartitions(storedOldWriterCopies);
            for (ResultPartitionWriter partition : storedOldWriterCopies) {
                taskEventDispatcher.unregisterPartition(partition.getPartitionId());
            }
        }
        storedOldWriterCopies = oldWriterCopies;
    }

    public void finish() {
        this.rescaleMeta = null;
        LOG.info(
                "++++++ taskRescaleManager finish, set meta to null for task "
                        + taskNameWithSubtaskAndId);
    }

    private static class TaskRescaleMeta {
        private final RescaleID rescaleId;
        private final RescaleOptions rescaleOptions;

        private final Collection<ResultPartitionDeploymentDescriptor>
                resultPartitionDeploymentDescriptors;
        private final Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors;

        private final ResultPartitionWriter[] newConsumableNotifyingPartitionWriters;

        TaskRescaleMeta(
                RescaleID rescaleId,
                RescaleOptions rescaleOptions,
                Collection<ResultPartitionDeploymentDescriptor>
                        resultPartitionDeploymentDescriptors,
                Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors) {

            this.rescaleId = checkNotNull(rescaleId);
            this.rescaleOptions = checkNotNull(rescaleOptions);

            this.resultPartitionDeploymentDescriptors =
                    checkNotNull(resultPartitionDeploymentDescriptors);
            this.inputGateDeploymentDescriptors = checkNotNull(inputGateDeploymentDescriptors);
            this.newConsumableNotifyingPartitionWriters =
                    new ResultPartitionWriter[resultPartitionDeploymentDescriptors.size()];
        }

        public RescaleID getRescaleId() {
            return rescaleId;
        }

        public RescaleOptions getRescaleOptions() {
            return rescaleOptions;
        }

        public Collection<ResultPartitionDeploymentDescriptor>
                getResultPartitionDeploymentDescriptors() {
            return resultPartitionDeploymentDescriptors;
        }

        public Collection<InputGateDeploymentDescriptor> getInputGateDeploymentDescriptors() {
            return inputGateDeploymentDescriptors;
        }

        public ResultPartitionWriter getNewPartitions(int index) {
            checkState(
                    index >= 0 && index < this.newConsumableNotifyingPartitionWriters.length,
                    "given index out of boundary");

            return newConsumableNotifyingPartitionWriters[index];
        }

        public void addNewPartitions(int index, ResultPartitionWriter partition) {
            checkState(
                    index >= 0 && index < this.newConsumableNotifyingPartitionWriters.length,
                    "given index out of boundary");

            newConsumableNotifyingPartitionWriters[index] = partition;
        }

        /**
         * Get matched InputGateDescriptor of the input gate
         *
         * <p>It just need to compare the `consumedResultId`. There will be never two
         * InputGateDeploymentDescriptor with the same `consumedResultId` which will actually be
         * sent to different parallel operator instance.
         *
         * <p>We should not compare the `consumedSubPartitionIndex` here since the original
         * partition type between previous operator and current may be `FORWARD`.
         *
         * <p>In that case, all its parallel operator instances has `consumedSubpartitionIndex`
         * zero. However, the new deployment descriptor may set the new `consumedSubpartitionIndex`
         * greater than zero which will cause gate would never find its new
         * InputGateDeploymentDescriptor.
         *
         * @param gate
         * @return
         */
        public InputGateDeploymentDescriptor getMatchedInputGateDescriptor(SingleInputGate gate) {
            List<InputGateDeploymentDescriptor> igdds = new ArrayList<>();
            for (InputGateDeploymentDescriptor igdd : inputGateDeploymentDescriptors) {
                if (gate.getConsumedResultId().equals(igdd.getConsumedResultId())) {
                    igdds.add(igdd);
                }
            }
            if (igdds.size() != 1) {
                throw new IllegalStateException(
                        "Cannot find matched InputGateDeploymentDescriptor");
            }
            gate.setConsumedSubpartitionIndex(igdds.get(0).getConsumedSubpartitionIndex());
            return igdds.get(0);
        }
    }
}
