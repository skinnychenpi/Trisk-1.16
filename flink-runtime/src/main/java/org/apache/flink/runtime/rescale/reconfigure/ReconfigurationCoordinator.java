package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ReconfigurationCoordinator extends AbstractCoordinator {

    public ReconfigurationCoordinator(JobGraph jobGraph, ExecutionGraph executionGraph) {
        super(jobGraph, (DefaultExecutionGraph) executionGraph);
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> synchronizeTasks(
            List<Tuple2<Integer, Integer>> taskList, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> synchronizeTasks(
            Map<Integer, List<Integer>> tasks, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }

    @Override
    public CompletableFuture<Void> resumeTasks() {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateTaskResources(int operatorID, int oldParallelism) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateTaskResources(
            Map<Integer, List<Integer>> tasks, Map<Integer, List<SlotID>> slotIds) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> updateKeyMapping(
            int destOpID, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> updateKeyMapping(
            Map<Integer, List<Integer>> tasks, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> updateState(
            int operatorID, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> updateState(
            Map<Integer, List<Integer>> tasks, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> updateFunction(
            int vertexID, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> updateFunction(
            Map<Integer, List<Integer>> tasks, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }
}
