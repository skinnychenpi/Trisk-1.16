package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AssignedKeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T>
        implements ConfigurableStreamPartitioner {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(AssignedKeyGroupStreamPartitioner.class);
    private final KeySelector<T, K> keySelector;

    private int maxParallelism;

    // map of (keyGroupId, subTaskIndex)
    private Map<Integer, Integer> assignKeyToOperator;

    public AssignedKeyGroupStreamPartitioner(
            KeySelector<T, K> keySelector,
            int maxParallelism,
            Map<Integer, List<Integer>> partitionAssignment) {
        Preconditions.checkArgument(maxParallelism > 0, "Number of key-groups must be > 0!");
        this.keySelector = Preconditions.checkNotNull(keySelector);
        this.maxParallelism = maxParallelism;
        this.assignKeyToOperator =
                getAssignKeyToOperator(
                        partitionAssignment); // 这里的assignKeyToOperator会生成一个partitionAssignment反向的map，也就是key变成了128个key group， 而value则是10个task,也就是说，现在上游的节点会根据自己的key group 来这里寻找它接下来应该去哪个节点
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        K key;
        try {
            key = keySelector.getKey(record.getInstance().getValue());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Could not extract key from " + record.getInstance().getValue(), e);
        }

        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
        int selectedChannel = assignKeyToOperator.get(keyGroup);
        LOG.info(
                "!!!!!!!!!! Record: ("
                        + key
                        + ","
                        + keyGroup
                        + ")"
                        + "channel: "
                        + selectedChannel);
        Preconditions.checkState(
                selectedChannel >= 0 && selectedChannel < numberOfChannels,
                "selected channel out of range , "
                        + metricsManager.getJobVertexId()
                        + ": "
                        + selectedChannel
                        + " / "
                        + numberOfChannels);

        record.getInstance().setKeyGroup(keyGroup);
        metricsManager.incRecordsOutKeyGroup(record.getInstance().getKeyGroup());

        return selectedChannel;
    }

    public Map<Integer, List<Integer>> getKeyMappingInfo(int parallelism) {
        Map<Integer, List<Integer>> keyStateAllocation = new HashMap<>();
        for (int channelIndex = 0; channelIndex < parallelism; channelIndex++) {
            keyStateAllocation.put(channelIndex, new ArrayList<>());
        }
        for (Integer key : assignKeyToOperator.keySet()) {
            keyStateAllocation.get(assignKeyToOperator.get(key)).add(key);
        }
        return keyStateAllocation;
    }

    @Override
    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return null;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "REHASHED";
    }

    @Override
    public void configure(int maxParallelism) {
        KeyGroupRangeAssignment.checkParallelismPreconditions(maxParallelism);
        this.maxParallelism = maxParallelism;
    }

    public void updateNewPartitionAssignment(Map<Integer, List<Integer>> partitionAssignment) {
        this.assignKeyToOperator = getAssignKeyToOperator(partitionAssignment);
    }

    private static Map<Integer, Integer> getAssignKeyToOperator(
            Map<Integer, List<Integer>> partitionAssignment) {
        Map<Integer, Integer> assignKeyToOperator = new HashMap<>();
        for (Integer subTaskIndex : partitionAssignment.keySet()) {
            for (Integer keyGroup : partitionAssignment.get(subTaskIndex)) {
                if (assignKeyToOperator.putIfAbsent(keyGroup, subTaskIndex) != null) {
                    throw new IllegalArgumentException("invalid partitionAssignment");
                }
            }
        }
        return assignKeyToOperator;
    }
}
