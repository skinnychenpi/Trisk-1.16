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
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.TaskActions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

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
         * <p>It just need to compare the `comsumedResultId`. There will be never two
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
        //        public InputGateDeploymentDescriptor getMatchedInputGateDescriptor(SingleInputGate
        // gate) {
        //            List<InputGateDeploymentDescriptor> igdds = new ArrayList<>();
        //            for (InputGateDeploymentDescriptor igdd : inputGateDeploymentDescriptors) {
        //                if (gate.getConsumedResultId().equals(igdd.getConsumedResultId())) {
        //                    igdds.add(igdd);
        //                }
        //            }
        //            if (igdds.size() != 1) {
        //                throw new IllegalStateException("Cannot find matched
        // InputGateDeploymentDescriptor");
        //            }
        //
        // gate.setConsumedSubpartitionIndex(igdds.get(0).getConsumedSubpartitionIndex());
        //            return igdds.get(0);
        //        }
    }
}
