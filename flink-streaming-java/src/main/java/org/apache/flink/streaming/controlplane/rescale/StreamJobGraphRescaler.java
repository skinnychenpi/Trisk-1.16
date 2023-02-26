/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.controlplane.rescale;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rescale.reconfigure.JobGraphRescaler;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.runtime.partitioner.AssignedKeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StreamJobGraphRescaler implements JobGraphRescaler {

    static final Logger LOG = LoggerFactory.getLogger(StreamJobGraphRescaler.class);

    protected final JobGraph jobGraph;
    protected final ClassLoader userCodeLoader;

    public StreamJobGraphRescaler(JobGraph jobGraph, ClassLoader userCodeLoader) {
        this.jobGraph = jobGraph;
        this.userCodeLoader = userCodeLoader;
    }

    @Override
    public Tuple2<List<JobVertexID>, List<JobVertexID>> rescale(
            JobVertexID id, int newParallelism, Map<Integer, List<Integer>> partitionAssignment) {
        JobVertex vertex = jobGraph.findVertexByID(id); // 这个时候并行度已经修改为10
        vertex.setParallelism(newParallelism);
        // 最终repartition方法会更新target上游节点的out路由，target下游节点的in路由.路由就是每一个key
        // group的id所导航到指定的task（在本例子中上游就是128 -> 10，下游就是128->1）
        return repartition(
                id,
                partitionAssignment); // partition assignment 就是新的key group dist， 也就是size为10的一个map，
        // 每个key对应自己的128个key group中的若干个
    }

    @Override
    public Tuple2<List<JobVertexID>, List<JobVertexID>> repartition(
            JobVertexID id, Map<Integer, List<Integer>> partitionAssignment) {
        List<JobVertexID> involvedUpstream = new LinkedList<>();
        List<JobVertexID> involvedDownstream = new LinkedList<>();
        JobVertex vertex = jobGraph.findVertexByID(id); // 此时并行度已经为10

        StreamConfig config = new StreamConfig(vertex.getConfiguration());

        // update upstream vertices and edges
        List<StreamEdge> targetInEdges = config.getInPhysicalEdges(userCodeLoader);

        for (JobEdge jobEdge : vertex.getInputs()) { // 这里vertex的input只有一个job
            JobVertex upstream =
                    jobEdge.getSource().getProducer(); // 提取出来的也就是源节点(整个图的source,也就是target的上游）
            involvedUpstream.add(upstream.getID());

            StreamConfig upstreamConfig = new StreamConfig(upstream.getConfiguration());

            List<StreamEdge> upstreamOutEdges = upstreamConfig.getOutEdgesInOrder(userCodeLoader);
            Tuple2<Map<String, StreamEdge>, StreamPartitioner<?>> updateResult =
                    updateEdgePartition(
                            jobEdge,
                            partitionAssignment,
                            upstreamOutEdges,
                            targetInEdges); // 这一步会修改上游节点和要修改并行度的节点的partition，也就是更新了上游节点生产的结果的路由表。这个表的key是128个key group中的某一个，value则是下游10个task中的某一个，这样就更新了路由。

            Map<String, StreamEdge> updatedEdges = updateResult.f0;
            StreamPartitioner<?> newPartitioner = updateResult.f1;
            // New Trisk 1.16 logic: We need to update the parallelism and partitioner of
            // NonChainedOutput
            updateNonChainedOutput(upstreamConfig, newPartitioner, vertex.getParallelism());

            upstreamConfig.setOutEdgesInOrder(upstreamOutEdges);
            updateAllOperatorsConfig(upstreamConfig, updatedEdges);
            // In Flink 1.16, everytime we modify the config, it is not serialized. We need to do it
            // by
            // ourselves.
            upstreamConfig.serializeAllConfigs();
        }
        config.setInPhysicalEdges(targetInEdges);

        // update downstream vertices and edges
        List<StreamEdge> targetOutEdges = config.getOutEdgesInOrder(userCodeLoader);

        for (IntermediateDataSet dataset : vertex.getProducedDataSets()) {
            for (JobEdge jobEdge : dataset.getConsumers()) {
                JobVertex downstream = jobEdge.getTarget();
                involvedDownstream.add(downstream.getID());

                StreamConfig downstreamConfig = new StreamConfig(downstream.getConfiguration());

                List<StreamEdge> downstreamInEdges =
                        downstreamConfig.getInPhysicalEdges(userCodeLoader);
                // TODO: targetVertex is not supposed to be modified

                Tuple2<Map<String, StreamEdge>, StreamPartitioner<?>> updateResult =
                        updateEdgePartition(
                                jobEdge,
                                null,
                                targetOutEdges,
                                downstreamInEdges); // 对于下游，updatedEdges返回的是一个空的map,也就是不需要修改target
                // edges到其下游节点的路由表？？
                Map<String, StreamEdge> updatedEdges = updateResult.f0;

                downstreamConfig.setInPhysicalEdges(downstreamInEdges);
                updateAllOperatorsConfig(downstreamConfig, updatedEdges);
                // In Flink 1.16, everytime we modify the config, it is not serialized. We need to
                // do it by
                // ourselves.
                downstreamConfig.serializeAllConfigs();
            }
        }
        config.setOutEdgesInOrder(targetOutEdges);
        // In Flink 1.16, everytime we modify the config, it is not serialized. We need to do it by
        // ourselves.
        config.serializeAllConfigs();
        return Tuple2.of(involvedUpstream, involvedDownstream);
    }

    @Override
    public String print(Configuration config) {
        StreamConfig streamConfig = new StreamConfig(config);

        return streamConfig.getOutEdgesInOrder(userCodeLoader).get(0).toString();
    }

    private Tuple2<Map<String, StreamEdge>, StreamPartitioner<?>> updateEdgePartition(
            JobEdge jobEdge,
            Map<Integer, List<Integer>> partitionAssignment,
            List<StreamEdge> upstreamOutEdges,
            List<StreamEdge> downstreamInEdges) {
        // Stream Edge 可以理解为Job之间的Edge
        Map<String, StreamEdge> updatedEdges = new HashMap<>();
        StreamPartitioner partitioner = null;
        for (StreamEdge outEdge : upstreamOutEdges) {
            for (StreamEdge inEdge : downstreamInEdges) {
                if (outEdge.equals(inEdge)) {

                    StreamPartitioner oldPartitioner = outEdge.getPartitioner();
                    StreamPartitioner newPartitioner = null;

                    if (oldPartitioner instanceof ForwardPartitioner) {
                        System.out.println("update vertex of ForwardPartitioner");
                        newPartitioner = new RebalancePartitioner();
                    } else if (oldPartitioner instanceof KeyGroupStreamPartitioner
                            && partitionAssignment != null) {
                        System.out.println("update vertex of KeyGroupStreamPartitioner");
                        KeyGroupStreamPartitioner keyGroupStreamPartitioner =
                                (KeyGroupStreamPartitioner) oldPartitioner;
                        // step in 来看看这个new里面的运行逻辑，有详细的注释
                        newPartitioner =
                                new AssignedKeyGroupStreamPartitioner(
                                        keyGroupStreamPartitioner.getKeySelector(),
                                        keyGroupStreamPartitioner.getMaxParallelism(),
                                        partitionAssignment);
                    } else if (oldPartitioner instanceof AssignedKeyGroupStreamPartitioner
                            && partitionAssignment != null) {
                        System.out.println("update vertex of AssignedKeyGroupStreamPartitioner");

                        ((AssignedKeyGroupStreamPartitioner) oldPartitioner)
                                .updateNewPartitionAssignment(partitionAssignment);
                        newPartitioner = oldPartitioner;
                    }
                    // TODO scaling: StreamEdge.edgeId contains partitioner string, update it?
                    // TODO scaling: what if RescalePartitioner

                    if (newPartitioner != null) {
                        partitioner = newPartitioner;
                        jobEdge.setDistributionPattern(DistributionPattern.ALL_TO_ALL);
                        jobEdge.setShipStrategyName(newPartitioner.toString());

                        System.out.println(
                                "Updating the partitioner of edge with ID@StreamJobGraphRescaler: "
                                        + outEdge.getEdgeId());

                        outEdge.setPartitioner(newPartitioner);
                        inEdge.setPartitioner(newPartitioner);

                        updatedEdges.put(inEdge.getEdgeId(), inEdge);
                    }
                }
            }
        }

        return Tuple2.of(updatedEdges, partitioner);
    }

    private void updateAllOperatorsConfig(
            StreamConfig chainEntryPointConfig, Map<String, StreamEdge> updatedEdges) {
        updateOperatorConfig(chainEntryPointConfig, updatedEdges);

        Map<Integer, StreamConfig> chainedConfigs =
                chainEntryPointConfig.getTransitiveChainedTaskConfigs(userCodeLoader);
        for (Map.Entry<Integer, StreamConfig> entry : chainedConfigs.entrySet()) {
            updateOperatorConfig(entry.getValue(), updatedEdges);
        }
        chainEntryPointConfig.setTransitiveChainedTaskConfigs(chainedConfigs);
    }

    private void updateOperatorConfig(
            StreamConfig operatorConfig, Map<String, StreamEdge> updatedEdges) {
        List<StreamEdge> nonChainedOutputs = operatorConfig.getNonChainedOutputs(userCodeLoader);
        for (int i = 0; i < nonChainedOutputs.size(); i++) {
            StreamEdge edge = nonChainedOutputs.get(i);
            if (updatedEdges.containsKey(edge.getEdgeId())) {
                nonChainedOutputs.set(i, updatedEdges.get(edge.getEdgeId()));
            }
        }
        operatorConfig.setNonChainedOutputs(nonChainedOutputs);
    }

    private void updateNonChainedOutput(
            StreamConfig operatorConfig, StreamPartitioner<?> newPartitioner, int newParallelism) {
        List<NonChainedOutput> nonChainedOutputs =
                operatorConfig.getVertexNonChainedOutputs(userCodeLoader);
        for (int i = 0; i < nonChainedOutputs.size(); i++) {
            NonChainedOutput output = nonChainedOutputs.get(i);
            if (output.getSourceNodeId() == operatorConfig.getVertexID()) {
                output.updatePartitioner(newPartitioner);
                output.updateConsumerParallelism(newParallelism);
                nonChainedOutputs.set(i, output);
            }
        }
        operatorConfig.setVertexNonChainedOutputs(nonChainedOutputs);
    }
}
