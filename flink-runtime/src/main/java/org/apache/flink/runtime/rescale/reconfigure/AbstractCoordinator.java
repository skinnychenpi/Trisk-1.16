package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.controlplane.ExecutionPlanAndJobGraphUpdaterFactory;
import org.apache.flink.runtime.controlplane.PrimitiveOperation;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptor;
import org.apache.flink.runtime.controlplane.abstraction.OperatorDescriptorVisitor;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rescale.RescaleID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

public abstract class AbstractCoordinator
        implements PrimitiveOperation<Map<Integer, Map<Integer, AbstractCoordinator.Diff>>> {

    protected JobGraph jobGraph;
    protected ExecutionGraph executionGraph;
    protected ClassLoader userCodeClassLoader;

    private ExecutionPlanAndJobGraphUpdaterFactory executionPlanAndJobGraphUpdaterFactory;

    private JobGraphUpdater jobGraphUpdater;
    protected WorkloadsAssignmentHandler workloadsAssignmentHandler;
    protected ExecutionPlan heldExecutionPlan;
    protected Map<Integer, OperatorID> operatorIDMap;

    // fields for deploy cancel tasks, OperatorID -> created/removed candidates
    protected volatile Map<Integer, List<ExecutionVertex>> removedCandidates;
    protected volatile Map<Integer, List<ExecutionVertex>> createdCandidates;

    // rescaleID should be maintained to identify number of reconfiguration that has been applied
    protected volatile RescaleID rescaleID;

    protected AbstractCoordinator(JobGraph jobGraph, DefaultExecutionGraph executionGraph) {
        this.jobGraph = jobGraph;
        this.executionGraph = executionGraph;
        this.userCodeClassLoader = executionGraph.getUserClassLoader();
        this.removedCandidates = new HashMap<>();
        this.createdCandidates = new HashMap<>();
    }

    public void setStreamRelatedInstanceFactory(
            ExecutionPlanAndJobGraphUpdaterFactory executionPlanAndJobGraphUpdaterFactory) {
        heldExecutionPlan =
                executionPlanAndJobGraphUpdaterFactory.createExecutionPlan(
                        jobGraph, executionGraph, userCodeClassLoader);
        workloadsAssignmentHandler = new WorkloadsAssignmentHandler(heldExecutionPlan);
        jobGraphUpdater =
                executionPlanAndJobGraphUpdaterFactory.createJobGraphUpdater(
                        jobGraph, userCodeClassLoader);
        operatorIDMap = jobGraphUpdater.getOperatorIDMap();
        this.executionPlanAndJobGraphUpdaterFactory = executionPlanAndJobGraphUpdaterFactory;
    }

    public ExecutionPlan getHeldExecutionPlanCopy() {
        // TODO: the object is not deep copied in this method, should use the deep copy defined in
        // execution plan
        ExecutionPlan executionPlan =
                executionPlanAndJobGraphUpdaterFactory.createExecutionPlan(
                        jobGraph, executionGraph, userCodeClassLoader);
        for (Iterator<OperatorDescriptor> it = executionPlan.getAllOperator(); it.hasNext(); ) {
            OperatorDescriptor descriptor = it.next();
            OperatorDescriptor heldDescriptor =
                    heldExecutionPlan.getOperatorByID(descriptor.getOperatorID());
            // make sure udf and other control attributes share the same reference so we could
            // identity the change if any
            OperatorDescriptor.ExecutionLogic heldAppLogic =
                    OperatorDescriptorVisitor.attachOperator(heldDescriptor).getApplicationLogic();
            OperatorDescriptor.ExecutionLogic appLogicCopy =
                    OperatorDescriptorVisitor.attachOperator(descriptor).getApplicationLogic();
            heldAppLogic.copyTo(appLogicCopy);
        }
        return executionPlan;
    }

    protected JobVertexID rawVertexIDToJobVertexID(int rawID) {
        OperatorID operatorID = operatorIDMap.get(rawID);
        if (operatorID == null) {
            return null;
        }
        for (JobVertex vertex : jobGraph.getVertices()) {
            List<OperatorIDPair> idPairs = vertex.getOperatorIDs();
            for (OperatorIDPair singlePair : idPairs) {
                if (singlePair.getGeneratedOperatorID().equals(operatorID)) {
                    return vertex.getID();
                }
            }
        }
        return null;
    }

    @Override
    public final CompletableFuture<Map<Integer, Map<Integer, Diff>>> prepareExecutionPlan(
            ExecutionPlan executionPlan) {
        rescaleID = RescaleID.generateNextID();
        Map<Integer, Map<Integer, Diff>> differenceMap = new HashMap<>();
        for (Iterator<OperatorDescriptor> it = executionPlan.getAllOperator(); it.hasNext(); ) {
            OperatorDescriptor descriptor = it.next();
            int operatorID = descriptor.getOperatorID();
            OperatorDescriptor heldDescriptor = heldExecutionPlan.getOperatorByID(operatorID);
            int oldParallelism = heldDescriptor.getParallelism();
            // loop until all change in this operator has been detected and sync
            List<Integer> changes =
                    analyzeOperatorDifference(heldDescriptor, descriptor); // 这个是operator层面的changes
            // TODO: this part is still operator-centric rather than task-centric
            // diff_id -> diff[udf, key state, key mapping, etc]
            Map<Integer, Diff> difference = new HashMap<>();
            // operator -> {diff_id -> diff}
            differenceMap.put(operatorID, difference);
            for (int changedPosition : changes) {
                try {
                    switch (changedPosition) {
                        case UDF: // change of logic for target tasks
                            OperatorDescriptor.ExecutionLogic heldAppLogic =
                                    OperatorDescriptorVisitor.attachOperator(heldDescriptor)
                                            .getApplicationLogic();
                            OperatorDescriptor.ExecutionLogic modifiedAppLogic =
                                    OperatorDescriptorVisitor.attachOperator(descriptor)
                                            .getApplicationLogic();
                            modifiedAppLogic.copyTo(heldAppLogic);
                            jobGraphUpdater.updateOperator(operatorID, heldAppLogic);
                            difference.put(UDF, AbstractCoordinator.ExecutionLogic.UDF);
                            break;
                            //						case PARALLELISM: // new tasks to be deployed
                            //							heldDescriptor.setParallelism(descriptor.getParallelism());
                            //							// next update job graph
                            //							JobVertexID jobVertexID =
                            // rawVertexIDToJobVertexID(heldDescriptor.getOperatorID());
                            //							JobVertex vertex = jobGraph.findVertexByID(jobVertexID);
                            //							vertex.setParallelism(heldDescriptor.getParallelism());
                            ////							difference.add(PARALLELISM);
                            //							break;
                        case KEY_STATE_ALLOCATION: // rebalance, rescale, placement
                            // convert the logical key mapping to Flink version partition assignment
                            OperatorWorkloadsAssignment operatorWorkloadsAssignment =
                                    workloadsAssignmentHandler.handleWorkloadsReallocate(
                                            operatorID,
                                            descriptor
                                                    .getKeyStateDistribution()); // 这个class内部自带能够将不连续的key group mapping转化为连续的区间，从而能够让task执行，等于会根据flink底层的要求再做一次remapping
                            difference.put(KEY_STATE_ALLOCATION, operatorWorkloadsAssignment);
                            boolean isRescale = false;
                            // if new tasks are deployed under current operator
                            //							if (heldDescriptor.getKeyStateAllocation().size() !=
                            // descriptor.getKeyStateAllocation().size()) {
                            if (operatorWorkloadsAssignment.isScaling()) {
                                isRescale = true;
                                heldDescriptor.setParallelism(descriptor.getParallelism());
                                // next update job graph
                                JobVertexID jobVertexID =
                                        rawVertexIDToJobVertexID(heldDescriptor.getOperatorID());
                                JobVertex vertex =
                                        jobGraph.findVertexByID(
                                                jobVertexID); // 执行到这里的时候 job的并行度仍然为2
                                vertex.setParallelism(heldDescriptor.getParallelism());
                                rescaleExecutionGraph(
                                        heldDescriptor.getOperatorID(),
                                        oldParallelism,
                                        operatorWorkloadsAssignment); // 利用jobVertex（operator）的partition信息，只更新下游的ExecutionEdges
                            }
                            heldDescriptor.updateKeyStateAllocation(
                                    descriptor
                                            .getKeyStateDistribution()); // 对其父算子的key mapping覆盖为该算子的
                            // key dist
                            // update the partition assignment of Flink JobGraph
                            updatePartitionAssignment(
                                    heldDescriptor,
                                    operatorWorkloadsAssignment,
                                    isRescale); // 这一个函数很重要，进去看有很多注释，他会从job层面对target job
                            // vertex的上下游节点的路由进行更新，(同时更新上游的ExecutionEdges???)
                            break;
                        case KEY_MAPPING: // update the key mapping of the tasks
                            // update key set will indirectly update key mapping, so we ignore this
                            // type of detected change here
                            difference.put(
                                    KEY_MAPPING, AbstractCoordinator.ExecutionLogic.KEY_MAPPING);
                            break;
                            // TODO: case resources
                        case NO_CHANGE:
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return FutureUtils.completedExceptionally(e);
                }
            }
        }
        // TODO: suspend checking StreamJobExecution for scale out
        //		final ExecutionPlan executionPlan = getHeldExecutionPlanCopy();
        //		for (Iterator<OperatorDescriptor> it = executionPlan.getAllOperatorDescriptor();
        // it.hasNext(); ) {
        //			OperatorDescriptor descriptor = it.next();
        //			OperatorDescriptor held =
        // heldExecutionPlan.getOperatorDescriptorByID(descriptor.getOperatorID());
        //			for (int changedPosition : analyzeOperatorDifference(held, descriptor)) {
        //				System.out.println("change no work:" + changedPosition);
        //				return FutureUtils.completedExceptionally(new Exception("change no work:" +
        // changedPosition));
        //			}
        //		}
        executionGraph.setJsonPlan(JsonPlanGenerator.generatePlan(jobGraph));
        return CompletableFuture.completedFuture(differenceMap);
    }

    private void rescaleExecutionGraph(
            int rawVertexID,
            int oldParallelism,
            OperatorWorkloadsAssignment operatorWorkloadsAssignment) {
        // scale up given ejv, update involved edges & partitions
        ExecutionJobVertex targetVertex =
                executionGraph.getJobVertex(rawVertexIDToJobVertexID(rawVertexID)); // 执行到这里的时候并行度为2
        JobVertex targetJobVertex =
                jobGraph.findVertexByID(
                        rawVertexIDToJobVertexID(rawVertexID)); // 这里的并行度已经是10了，在144行更新了
        Preconditions.checkNotNull(targetVertex, "can not found target vertex");
        List<JobVertexID> updatedDownstream =
                heldExecutionPlan.getOperatorByID(rawVertexID).getChildren().stream()
                        .map(child -> rawVertexIDToJobVertexID(child.getOperatorID()))
                        .collect(Collectors.toList());

        List<JobVertexID> updatedUpstream =
                heldExecutionPlan.getOperatorByID(rawVertexID).getParents().stream()
                        .map(child -> rawVertexIDToJobVertexID(child.getOperatorID()))
                        .collect(Collectors.toList());

        if (oldParallelism < targetJobVertex.getParallelism()) {
            // scale out, we assume every time only one scale out will be called. Otherwise it will
            // subsitute current candidates
            this.createdCandidates.put(
                    rawVertexID,
                    targetVertex.scaleOut(
                            executionGraph.getExecutionHistorySizeLimit(),
                            executionGraph.getRpcTimeout(),
                            System.currentTimeMillis(),
                            null)); // 这里在execution graph对target上游的output进行了execution
            // edge的修改（2个变成10个）+ 修改了target execution vertex（从2个变成10个）

            for (JobVertexID downstreamID : updatedDownstream) {
                ExecutionJobVertex downstream =
                        executionGraph.getJobVertex(downstreamID); // 检查一下这个时候的
                // downstream的input的并行度是不是已经更新为10了？ANS：这个时候target的partition已经是10了，downStream还没
                if (downstream != null) {
                    downstream.reconnectWithUpstream(
                            targetVertex
                                    .getProducedDataSets()); // 这个函数利用传入的jobEdges的信息将target下游的节点的input ExecutionEdges 设定为10，并且更新到最新的partition
                }
            }
        } else if (oldParallelism > targetJobVertex.getParallelism()) {
            // scale in
            List<Integer> removedTaskIds = operatorWorkloadsAssignment.getRemovedSubtask();
            checkState(removedTaskIds.size() > 0);
            this.removedCandidates.put(rawVertexID, targetVertex.scaleIn(removedTaskIds));

            // scale in need to update upstream consumers
            for (JobVertexID upstreamID : updatedUpstream) {
                ExecutionJobVertex upstream = executionGraph.getJobVertex(upstreamID);
                assert upstream != null;
                upstream.resetProducedDataSets();
                targetVertex.reconnectWithUpstream(upstream.getProducedDataSets());
            }

            for (JobVertexID downstreamID : updatedDownstream) {
                ExecutionJobVertex downstream = executionGraph.getJobVertex(downstreamID);
                assert downstream != null;
                downstream.reconnectWithUpstream(targetVertex.getProducedDataSets());
            }
        } else {
            // placement
            List<Integer> createdTaskIds = operatorWorkloadsAssignment.getCreatedSubtask();
            this.createdCandidates.put(
                    rawVertexID,
                    targetVertex.scaleOut(
                            executionGraph.getExecutionHistorySizeLimit(),
                            executionGraph.getRpcTimeout(),
                            System.currentTimeMillis(),
                            createdTaskIds));

            for (JobVertexID downstreamID : updatedDownstream) {
                ExecutionJobVertex downstream = executionGraph.getJobVertex(downstreamID);
                if (downstream != null) {
                    downstream.reconnectWithUpstream(targetVertex.getProducedDataSets());
                }
            }

            executionGraph.updateNumOfTotalVertices();

            List<Integer> removedTaskIds = operatorWorkloadsAssignment.getRemovedSubtask();
            this.removedCandidates.put(rawVertexID, targetVertex.scaleIn(removedTaskIds));

            // scale in need to update upstream consumers
            for (JobVertexID upstreamID : updatedUpstream) {
                ExecutionJobVertex upstream = executionGraph.getJobVertex(upstreamID);
                assert upstream != null;
                upstream.resetProducedDataSets();
                targetVertex.reconnectWithUpstream(upstream.getProducedDataSets());
            }

            for (JobVertexID downstreamID : updatedDownstream) {
                ExecutionJobVertex downstream = executionGraph.getJobVertex(downstreamID);
                assert downstream != null;
                downstream.reconnectWithUpstream(targetVertex.getProducedDataSets());
            }
        }
        executionGraph.updateNumOfTotalVertices();
    }

    private void updatePartitionAssignment(
            OperatorDescriptor heldDescriptor,
            OperatorWorkloadsAssignment operatorWorkloadsAssignment,
            boolean isRescale) {
        if (isRescale) {
            jobGraphUpdater.rescale(
                    rawVertexIDToJobVertexID(heldDescriptor.getOperatorID()),
                    heldDescriptor.getParallelism(),
                    operatorWorkloadsAssignment.getPartitionAssignment());
        } else {
            jobGraphUpdater.repartition(
                    rawVertexIDToJobVertexID(heldDescriptor.getOperatorID()),
                    operatorWorkloadsAssignment.getPartitionAssignment());
        }
    }

    private List<Integer> analyzeOperatorDifference(
            OperatorDescriptor self, OperatorDescriptor modified) {
        List<Integer> results = new LinkedList<>();
        OperatorDescriptor.ExecutionLogic heldAppLogic =
                OperatorDescriptorVisitor.attachOperator(self).getApplicationLogic();
        OperatorDescriptor.ExecutionLogic modifiedAppLogic =
                OperatorDescriptorVisitor.attachOperator(modified).getApplicationLogic();
        if (!heldAppLogic.equals(modifiedAppLogic)) {
            results.add(UDF);
        }
        if (self.getParallelism() != modified.getParallelism()) {
            results.add(PARALLELISM);
        }
        if (compareKeyStateAllocation(
                self.getKeyStateDistribution(), modified.getKeyStateDistribution())) {
            results.add(KEY_STATE_ALLOCATION);
        }
        if (compareOutputKeyMapping(self.getKeyMapping(), modified.getKeyMapping())) {
            results.add(KEY_MAPPING);
        }
        Collections.sort(results);
        return results;
    }

    /**
     * @param map1
     * @param map2
     * @return true if there are different
     */
    private boolean compareOutputKeyMapping(
            Map<Integer, Map<Integer, List<Integer>>> map1,
            Map<Integer, Map<Integer, List<Integer>>> map2) {
        if (map1.size() != map2.size()) {
            return true;
        }
        for (Integer key : map1.keySet()) {
            Map<Integer, List<Integer>> innerMap = map2.get(key);
            if (innerMap == null || compareKeyStateAllocation(map1.get(key), innerMap)) {
                return true;
            }
        }
        return false;
    }

    private boolean compareKeyStateAllocation(
            Map<Integer, List<Integer>> map1, Map<Integer, List<Integer>> map2) {
        if (map1.size() != map2.size()) {
            return true;
        }

        for (Integer key : map1.keySet()) {
            List<Integer> value = map2.get(key);
            if (value == null || compareIntList(map1.get(key), value)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param list1
     * @param list2
     * @return true if they are not equals
     */
    static boolean compareIntList(List<Integer> list1, List<Integer> list2) {
        if (list1.size() != list2.size()) {
            return true;
        }
        return !Arrays.equals(list1.stream().sorted().toArray(), list2.stream().sorted().toArray());
    }

    @Override
    @Deprecated
    public CompletableFuture<Acknowledge> updateFunction(
            JobGraph jobGraph, JobVertexID targetVertexID, OperatorID operatorID) {
        throw new UnsupportedOperationException();
    }

    // the value means priority, the higher, the later should be resolve
    public static final int UDF = 0;
    public static final int PARALLELISM = 1;
    public static final int KEY_STATE_ALLOCATION = 2;
    public static final int KEY_MAPPING = 3;
    public static final int NO_CHANGE = 4;

    public interface Diff {}

    enum ExecutionLogic implements Diff {
        UDF,
        KEY_MAPPING
    }
}
