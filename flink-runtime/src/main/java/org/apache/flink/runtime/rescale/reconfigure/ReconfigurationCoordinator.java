package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StateAssignmentOperation;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphException;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.rescale.RescaleID;
import org.apache.flink.runtime.rescale.RescaleOptions;
import org.apache.flink.runtime.rescale.RescalepointAcknowledgeListener;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class ReconfigurationCoordinator extends AbstractCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ReconfigurationCoordinator.class);
    private SynchronizeOperation currentSyncOp = null;

    public ReconfigurationCoordinator(JobGraph jobGraph, ExecutionGraph executionGraph) {
        super(jobGraph, (DefaultExecutionGraph) executionGraph);
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> synchronizeTasks(
            List<Tuple2<Integer, Integer>> taskList, Map<Integer, Map<Integer, Diff>> message) {
        return null;
    }

    /**
     * get actual affected execution vertices in Flink
     *
     * @param ejv
     */
    private List<ExecutionVertex> getAffectedVertices(ExecutionJobVertex ejv) {
        return new ArrayList<>(Arrays.asList(ejv.getTaskVertices()));
    }

    @Override
    public CompletableFuture<Map<Integer, Map<Integer, Diff>>> synchronizeTasks(
            Map<Integer, List<Integer>> tasks, Map<Integer, Map<Integer, Diff>> diff) {
        System.out.println("start synchronizing..." + tasks);
        LOG.info("++++++ start synchronizing..." + tasks);
        // stateful tasks, inject barrier
        SynchronizeOperation syncOp = new SynchronizeOperation(tasks);
        try {
            CompletableFuture<Map<OperatorID, OperatorState>> collectedOperatorStateFuture =
                    syncOp.sync();
            // some check related here
            currentSyncOp = syncOp;
            return collectedOperatorStateFuture.thenApply(
                    state -> {
                        System.out.println("synchronizeTasks successful");
                        LOG.debug("synchronizeTasks successful");
                        // if update state is needed, try to re-assign state among those tasks
                        checkNotNull(
                                diff,
                                "error while getting difference between old and new execution plan");
                        for (Integer operatorID : diff.keySet()) {
                            if (diff.get(operatorID).containsKey(KEY_STATE_ALLOCATION)) {
                                checkNotNull(
                                        syncOp,
                                        "no state collected currently, have you synchronized first?");
                                JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
                                ExecutionJobVertex executionJobVertex =
                                        executionGraph.getJobVertex(jobVertexID);
                                Preconditions.checkNotNull(
                                        executionJobVertex,
                                        "Execution job vertex not found: " + jobVertexID);
                                OperatorWorkloadsAssignment remappingAssignment =
                                        (OperatorWorkloadsAssignment)
                                                diff.get(operatorID)
                                                        .get(
                                                                AbstractCoordinator
                                                                        .KEY_STATE_ALLOCATION);
                                StateAssignmentOperation stateAssignmentOperation =
                                        new StateAssignmentOperation(
                                                syncOp.checkpointId,
                                                Collections.singleton(executionJobVertex),
                                                state,
                                                true);
                                stateAssignmentOperation.setForceRescale(true);
                                stateAssignmentOperation.setRedistributeStrategy(
                                        remappingAssignment); // Currently the strategy is set but
                                // we haven't used the strategy for
                                // assigning states in
                                // stateAssignmentOperation.
                                LOG.info("++++++ start to assign states " + state);
                                stateAssignmentOperation.assignStates();
                                // can safely sync some old parameters because all modifications in
                                // JobMaster is completed.
                                executionJobVertex.syncOldConfigInfo();
                                LOG.info("++++++ assign states completed " + state);
                            }
                        }
                        return diff;
                    });
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
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
            Map<Integer, List<Integer>> tasks, Map<Integer, Map<Integer, Diff>> diff) {
        System.out.println("update mapping...");
        LOG.info("++++++ update Key Mapping");

        int targetOperatorID = tasks.keySet().iterator().next();
        // update key group range in target stream
        JobVertexID targetJobVertexID = rawVertexIDToJobVertexID(targetOperatorID);
        ExecutionJobVertex targetJobVertex = executionGraph.getJobVertex(targetJobVertexID); // This will be the second operator: FlatMap
        checkNotNull(targetJobVertex, "Execution job vertex not found: " + targetJobVertexID);
        final SynchronizeOperation syncOp = this.currentSyncOp;

        final List<CompletableFuture<Void>> rescaleCandidatesFutures = new ArrayList<>();
        try {
            // update the result partition of upstream, and also the downstream input gates
            for (OperatorDescriptor upstreamOperator : heldExecutionPlan.getOperatorByID(targetOperatorID).getParents()) {
                // todo some partitions may not need modified, for example, broad cast partitioner
                rescaleCandidatesFutures
                        .add(updatePartitions(upstreamOperator.getOperatorID(), rescaleID)
                                        .thenCompose(o -> updateDownstreamGates(upstreamOperator.getOperatorID()))
//							.thenCompose(o -> {
//								// check should we resume those tasks
//								Map<Integer, Diff> diffMap = diff.get(upstreamOperator.getOperatorID());
//								diffMap.remove(AbstractCoordinator.KEY_MAPPING);
//								if (diffMap.isEmpty() && syncOp != null) {
//									return syncOp.resumeTasks(Collections.singletonList(Tuple2.of(upstreamOperator.getOperatorID(), -1)));
//								}
//								return CompletableFuture.completedFuture(null);
//							})
                        );
                // check should we resume those tasks
                // if the diffMap of current operation is empty, we should resume the operator
                Map<Integer, Diff> upstreamDiffMap = diff.get(upstreamOperator.getOperatorID());
                upstreamDiffMap.remove(AbstractCoordinator.KEY_MAPPING);
                if (upstreamDiffMap.isEmpty() && syncOp != null) {
                    syncOp.resumeTasks(Collections.singletonList(Tuple2.of(upstreamOperator.getOperatorID(), -1)));
                }
            }

            OperatorWorkloadsAssignment remappingAssignment = workloadsAssignmentHandler.getHeldOperatorWorkloadsAssignment(targetOperatorID);
            List<Tuple2<Integer, Integer>> notModifiedList = new ArrayList<>();
            for (int subtaskIndex = 0; subtaskIndex < targetJobVertex.getParallelism(); subtaskIndex++) {
                ExecutionVertex vertex = targetJobVertex.getTaskVertices()[subtaskIndex];
                Execution execution = vertex.getCurrentExecutionAttempt();
                if (execution != null && execution.getState() == ExecutionState.RUNNING) {
                    rescaleCandidatesFutures.add(execution.scheduleRescale(
                            rescaleID,
                            RescaleOptions.RESCALE_KEYGROUP_RANGE_ONLY,
                            remappingAssignment.getAlignedKeyGroupRange(subtaskIndex)));
                    // if is not a modified task and do not need to update partitions, then resume.
                    if (!remappingAssignment.isTaskModified(subtaskIndex)) {
                        notModifiedList.add(Tuple2.of(targetOperatorID, subtaskIndex));
                    }
                } else {
                    vertex.assignKeyGroupRange(remappingAssignment.getAlignedKeyGroupRange(subtaskIndex));
                }
            }

            // Before resume those tasks, should check whether the update on the task has been completed, using diffmap
            // But diffmap is operator-centric, and is unable to check whether the task should be resumed.
            // To solve this problem, we check the remapping assignment of the operator, which stores which task is modified
            // The unmodified tasks can be resumed when the diffMap have only the remapping assignment and no other configs to update.
            // TODO: if there are multiple operations on a task, we update configs one by one
            //  it is possible that each operation happend concurrently
            //  but this can be resolved by calling resume for all blocked tasks again after all update completed
            Map<Integer, Diff> diffMap = diff.get(targetOperatorID);
            if (diffMap.containsKey(AbstractCoordinator.KEY_MAPPING)
                    || diffMap.containsKey(AbstractCoordinator.UDF)) {
                if (remappingAssignment.isScaling()) {
                    try { // update partition and downstream gates if there are tasks to be scaled out/in
                        updatePartitions(targetOperatorID, rescaleID)
                                // downstream gates need to find out the upstream partitions, do not update downstream gates before upstream has updated
//						.thenCompose(o -> updateDownstreamGates(targetOperatorID))
                                .thenAccept(o -> syncOp.resumeTasks(notModifiedList));
                    } catch (ExecutionGraphException e) {
                        e.printStackTrace();
                    }
                } else {
                    syncOp.resumeTasks(notModifiedList);
                }
            }

            CompletableFuture<Void> finishFuture = FutureUtils.completeAll(rescaleCandidatesFutures);
//			finishFuture.thenAccept(
////			return finishFuture.thenApply(
//				o -> {
//					for (OperatorDescriptor upstreamOperator : heldExecutionPlan.getOperatorDescriptorByID(targetOperatorID).getParents()) {
//						// check should we resume those tasks
//						Map<Integer, Diff> diffMap = diff.get(upstreamOperator.getOperatorID());
//						diffMap.remove(AbstractCoordinator.KEY_MAPPING);
//						if (diffMap.isEmpty() && syncOp != null) {
//							syncOp.resumeTasks(Collections.singletonList(Tuple2.of(upstreamOperator.getOperatorID(), -1)));
//						}
//					}
////					return diff;
//				}
//			);
            // the tasks can be resumed asynchrouously
            return finishFuture.thenApply(o -> {
                LOG.info("++++++ completed key mapping");
                return diff;
            });
        } catch (ExecutionGraphException e) {
            return FutureUtils.completedExceptionally(e);
        }
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

    private CompletableFuture<Void> updatePartitions(int operatorID, RescaleID rescaleID) throws ExecutionGraphException {
        // update result partition
        JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
        ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);
        Preconditions.checkNotNull(jobVertex, "Execution job vertex not found: " + jobVertexID);
        List<CompletableFuture<Void>> updatePartitionsFuture = new ArrayList<>();
        if (!jobVertex.getInputs().isEmpty() || currentSyncOp == null) {
            // the source operator has updated its partitions during synchronization, skip source operator partition update
            jobVertex.cleanBeforeRescale();
            for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
                Execution execution = vertex.getCurrentExecutionAttempt();
                if (!vertex.getRescaleId().equals(rescaleID) && execution != null && execution.getState() == ExecutionState.RUNNING) {
                    execution.updateProducedPartitions(rescaleID);
                    updatePartitionsFuture.add(execution.scheduleRescale(rescaleID, RescaleOptions.RESCALE_PARTITIONS_ONLY, null));
                }
            }
        }
        return FutureUtils.completeAll(updatePartitionsFuture);
    }

    private CompletableFuture<Void> updateDownstreamGates(int operatorID) {
        List<CompletableFuture<Void>> updateGatesFuture = new ArrayList<>();
        // update input gates in child stream of source op
        for (OperatorDescriptor downstreamOperator : heldExecutionPlan.getOperatorByID(operatorID).getChildren()) {
            try {
                updateGates(downstreamOperator.getOperatorID(), updateGatesFuture); // 这里的downstreamOperator 是 FlatMap
            } catch (ExecutionGraphException e) {
                e.printStackTrace();
            }
        }
        return FutureUtils.completeAll(updateGatesFuture);
    }

    private void updateGates(int operatorID, List<CompletableFuture<Void>> futureList) throws ExecutionGraphException {
        JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
        ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);
        Preconditions.checkNotNull(jobVertex, "Execution job vertex not found: " + jobVertexID);
        for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
            Execution execution = vertex.getCurrentExecutionAttempt();
            if (execution != null && execution.getState() == ExecutionState.RUNNING) {
                futureList.add(execution.scheduleRescale(rescaleID, RescaleOptions.RESCALE_GATES_ONLY, null));
            }
        }
    }

    private class SynchronizeOperation implements RescalepointAcknowledgeListener {

        private final Set<ExecutionAttemptID> notYetAcknowledgedTasks = new HashSet<>();

        private final List<JobVertexID> jobVertexIdList;
        private final List<Tuple2<Integer, Integer>> pausedTasks;
        private final Object lock = new Object();

        private final CompletableFuture<Map<OperatorID, OperatorState>> finishedFuture;

        private long checkpointId;

        SynchronizeOperation(List<Tuple2<Integer, Integer>> taskList) {
            this.jobVertexIdList =
                    taskList.stream()
                            .map(t -> rawVertexIDToJobVertexID(t.f0))
                            .collect(Collectors.toList());
            this.pausedTasks = convertToNoneNegativeOffsetList(taskList);
            finishedFuture = new CompletableFuture<>();
        }

        SynchronizeOperation(Map<Integer, List<Integer>> tasks) {
            List<Tuple2<Integer, Integer>> taskList =
                    tasks.keySet().stream().map(t -> Tuple2.of(t, -1)).collect(Collectors.toList());
            //			for (int rawId : tasks.keySet()) {
            //				taskList.addAll(tasks.get(rawId).stream()
            //					.map(t -> Tuple2.of(rawId, t))
            //					.collect(Collectors.toList()));
            //			}
            this.jobVertexIdList =
                    taskList.stream()
                            .map(t -> rawVertexIDToJobVertexID(t.f0))
                            .collect(Collectors.toList());

            this.pausedTasks = convertToNoneNegativeOffsetList(taskList);
            finishedFuture = new CompletableFuture<>();
        }

        private CompletableFuture<Map<OperatorID, OperatorState>> sync()
                throws ExecutionGraphException {
            // add needed to acknowledge tasks
            List<CompletableFuture<Void>> affectedExecutionPrepareSyncFutures = new LinkedList<>();
            for (JobVertexID jobVertexId : jobVertexIdList) {
                ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);
                checkNotNull(executionJobVertex, "can not find the job vertex" + jobVertexId);
                List<ExecutionVertex> affectedVertices =
                        getAffectedVertices(
                                executionJobVertex); // source(first loop) + 10 * Splitter
                // FlatMap(second loop) + 1 * filter(third
                // loop)
                if (executionJobVertex.getInputs().isEmpty()) {
                    // this is source task vertex
                    affectedExecutionPrepareSyncFutures.add(
                            pauseSourceStreamTask(
                                    executionJobVertex,
                                    affectedVertices)); // For the source task, it just uploads the
                    // task configuration to latest. I can't
                    // find where it is paused.
                } else {
                    // sync affected existing task vertices
                    affectedVertices.stream()
                            .map(ExecutionVertex::getCurrentExecutionAttempt)
                            .filter(
                                    execution ->
                                            execution != null
                                                    && (execution.getState()
                                                                    == ExecutionState.RUNNING
                                                            || execution.getState()
                                                                    == ExecutionState.DEPLOYING))
                            .forEach(
                                    execution -> {
                                        affectedExecutionPrepareSyncFutures.add(
                                                execution.scheduleForInterTaskSync(
                                                        TaskOperatorManager
                                                                .NEED_SYNC_REQUEST)); // Trace to
                                        // TaskExecutor.java -> this just set up a flag for related
                                        // tasks.
                                        notYetAcknowledgedTasks.add(execution.getAttemptId());
                                    });
                }
            } // 在这一步处理之后，affectedExecutionPrepareSyncFutures的size是4， 也就是source + 2 * Map + 1 *
            // Filter。只有这些task才会被调用相应的函数，也就是pauseSourceStreamTask以及scheduleForInterTaskSync。

            // sync tasks to be removed in the new configuration
            // because it was removed from executionJobVertex, need to use the removed candidates to
            // store them
            for (Map.Entry<Integer, List<ExecutionVertex>> entry : removedCandidates.entrySet()) {
                for (ExecutionVertex vertex : entry.getValue()) {
                    Execution execution = vertex.getCurrentExecutionAttempt();
                    affectedExecutionPrepareSyncFutures.add(
                            execution.scheduleForInterTaskSync(
                                    TaskOperatorManager.NEED_SYNC_REQUEST));
                    notYetAcknowledgedTasks.add(execution.getAttemptId());
                }
            }

            // make affected tasks prepare synchronization
            FutureUtils.completeAll(affectedExecutionPrepareSyncFutures)
                    .thenRunAsync(
                            () -> {
                                try {
                                    CheckpointCoordinator checkpointCoordinator =
                                            executionGraph.getCheckpointCoordinator();
                                    checkNotNull(
                                            checkpointCoordinator,
                                            "do not have checkpointCoordinator");
                                    checkpointCoordinator.stopCheckpointScheduler();
                                    checkpointCoordinator.setRescalepointAcknowledgeListener(this);
                                    // temporary use rescale point
                                    System.out.println("send barrier...");
                                    checkpointCoordinator.triggerRescalePoint(
                                            System.currentTimeMillis()); // HERE IS THE REAL SYNC!!!
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            });
            return finishedFuture;
        }

        private CompletableFuture<Void> pauseSourceStreamTask(
                ExecutionJobVertex executionJobVertex, List<ExecutionVertex> affectedVertices)
                throws ExecutionGraphException {

            List<CompletableFuture<Void>> futureList = new ArrayList<>();
            executionJobVertex.cleanBeforeRescale();
            for (ExecutionVertex executionVertex : affectedVertices) {
                Execution execution = executionVertex.getCurrentExecutionAttempt();
                if (execution != null && execution.getState() == ExecutionState.RUNNING) {
                    execution.updateProducedPartitions(rescaleID);
                    futureList.add(
                            execution.scheduleRescale(
                                    rescaleID, RescaleOptions.PREPARE_ONLY, null));
                    notYetAcknowledgedTasks.add(execution.getAttemptId());
                }
            }
            return FutureUtils.completeAll(futureList);
        }

        private CompletableFuture<Void> resumeAll() {
            return resumeTasks(pausedTasks);
        }

        private CompletableFuture<Void> resumeTasks(List<Tuple2<Integer, Integer>> taskList) {
            System.out.println("resuming..." + taskList);
            taskList = convertToNoneNegativeOffsetList(taskList);
            List<CompletableFuture<Void>> affectedExecutionPrepareSyncFutures = new LinkedList<>();
            boolean isSourceResumed = false;
            for (Tuple2<Integer, Integer> taskID : taskList) {
                synchronized (pausedTasks) {
                    if (!this.pausedTasks.remove(taskID)) {
                        continue;
                    }
                }
                JobVertexID jobVertexID = rawVertexIDToJobVertexID(taskID.f0);
                ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexID);
                checkNotNull(executionJobVertex);
                ExecutionVertex operatedVertex = executionJobVertex.getTaskVertices()[taskID.f1];

                if (executionJobVertex.getInputs().isEmpty()) {
                    // To resume source task, we need to update its down stream gates,
                    // in some case, this may have been done during update mapping.
                    //
                    // Since we could ensure that we will only update those partition id changed
                    // gates, thus in this case, it is ok cause resumeSourceStreamTask will do
                    // nothing
                    if (!isSourceResumed) { // only need to update once for downstream tasks
                        affectedExecutionPrepareSyncFutures.add(updateDownstreamGates(taskID.f0));
                    }
                    isSourceResumed = true;
                } else {
                    Execution execution = operatedVertex.getCurrentExecutionAttempt();
                    affectedExecutionPrepareSyncFutures.add(
                            execution.scheduleForInterTaskSync(
                                    TaskOperatorManager.NEED_RESUME_REQUEST));
                }
            }
            // make affected task resume
            return FutureUtils.completeAll(affectedExecutionPrepareSyncFutures);
        }

        private CompletableFuture<Void> updateDownstreamGates(int operatorID) {
            List<CompletableFuture<Void>> updateGatesFuture = new ArrayList<>();
            // update input gates in child stream of source op
            for (OperatorDescriptor downstreamOperator :
                    heldExecutionPlan.getOperatorByID(operatorID).getChildren()) {
                try {
                    updateGates(
                            downstreamOperator.getOperatorID(),
                            updateGatesFuture); // 这里的downstreamOperator 是 FlatMap
                } catch (ExecutionGraphException e) {
                    e.printStackTrace();
                }
            }
            return FutureUtils.completeAll(updateGatesFuture);
        }

        private void updateGates(int operatorID, List<CompletableFuture<Void>> futureList)
                throws ExecutionGraphException {
            JobVertexID jobVertexID = rawVertexIDToJobVertexID(operatorID);
            ExecutionJobVertex jobVertex = executionGraph.getJobVertex(jobVertexID);
            Preconditions.checkNotNull(jobVertex, "Execution job vertex not found: " + jobVertexID);
            for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
                Execution execution = vertex.getCurrentExecutionAttempt();
                if (execution != null && execution.getState() == ExecutionState.RUNNING) {
                    futureList.add(
                            execution.scheduleRescale(
                                    rescaleID, RescaleOptions.RESCALE_GATES_ONLY, null));
                }
            }
        }

        private List<Tuple2<Integer, Integer>> convertToNoneNegativeOffsetList(
                List<Tuple2<Integer, Integer>> vertexIDList) {
            List<Tuple2<Integer, Integer>> convertedVertexIDList =
                    new ArrayList<>(vertexIDList.size());
            for (Tuple2<Integer, Integer> vertexID : vertexIDList) {
                ExecutionJobVertex executionJobVertex =
                        executionGraph.getJobVertex(rawVertexIDToJobVertexID(vertexID.f0));
                checkNotNull(
                        executionJobVertex, "can not find the job vertex" + vertexID.toString());
                if (vertexID.f1 < 0) {
                    convertedVertexIDList.addAll(
                            Arrays.stream(executionJobVertex.getTaskVertices())
                                    .filter(
                                            e ->
                                                    e.getCurrentExecutionAttempt() != null
                                                                    && (e.getCurrentExecutionAttempt()
                                                                                    .getState()
                                                                            == ExecutionState
                                                                                    .RUNNING)
                                                            || e.getCurrentExecutionAttempt()
                                                                            .getState()
                                                                    == ExecutionState.DEPLOYING)
                                    .map(e -> Tuple2.of(vertexID.f0, e.getParallelSubtaskIndex()))
                                    .collect(Collectors.toList()));
                } else {
                    //				checkArgument(vertexID.f1 < executionJobVertex.getParallelism(), "offset
                    // out of boundary");
                    boolean isValid = false;
                    for (ExecutionVertex vertex : executionJobVertex.getTaskVertices()) {
                        if (vertexID.f1 == vertex.getParallelSubtaskIndex()) {
                            convertedVertexIDList.add(vertexID);
                            isValid = true;
                            break;
                        }
                    }
                    // if the task is to be removed, it is a valid task but is not a task to be
                    // resumed in the future
                    // we can safely remove the task from the pausedtask, because we will update
                    // pause all removed tasks next.
                    if (!isValid) {
                        for (ExecutionVertex vertex : removedCandidates.get(vertexID.f0)) {
                            if (vertexID.f1 == vertex.getParallelSubtaskIndex()) {
                                isValid = true;
                                break;
                            }
                        }
                    }
                    checkArgument(
                            isValid, "subtaskindex is not found in executionvertex", vertexID);
                }
            }
            return convertedVertexIDList;
        }

        @Override
        public void onReceiveRescalepointAcknowledge(
                ExecutionAttemptID attemptID, PendingCheckpoint checkpoint) {
            if (checkpointId == checkpoint.getCheckpointId()) {
                CompletableFuture.runAsync(
                        () -> {
                            LOG.info("++++++ Received Rescalepoint Acknowledgement:" + attemptID);
                            try {
                                synchronized (lock) {
                                    if (notYetAcknowledgedTasks.isEmpty()) {
                                        // late come in snapshot, ignore it
                                        return;
                                    }
                                    notYetAcknowledgedTasks.remove(attemptID);

                                    if (notYetAcknowledgedTasks.isEmpty()) {
                                        LOG.info("++++++ handle operator states");
                                        CheckpointCoordinator checkpointCoordinator =
                                                executionGraph.getCheckpointCoordinator();
                                        checkNotNull(checkpointCoordinator);
                                        if (checkpointCoordinator
                                                .isPeriodicCheckpointingConfigured()) {
                                            LOG.info("++++++ resume checkpoint coordinator");
                                            checkpointCoordinator.startCheckpointScheduler();
                                        }
                                        LOG.info(
                                                "++++++ received operator states"
                                                        + checkpoint.getOperatorStates()
                                                        + " : "
                                                        + finishedFuture);
                                        finishedFuture.complete(
                                                new HashMap<>(checkpoint.getOperatorStates()));
                                    }
                                }
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        });
            }
        }

        @Override
        public void setCheckpointId(long checkpointId) {
            this.checkpointId = checkpointId;
            System.out.println("trigger rescale point with check point id:" + checkpointId);
        }
    }
}
