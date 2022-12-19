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

package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.controlplane.ExecutionPlanAndJobGraphUpdaterFactory;
import org.apache.flink.runtime.controlplane.abstraction.ExecutionPlan;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerId;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rescale.JobRescaleAction;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.registration.JobManagerRegistration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.ReconfigurationExecutor;
import org.apache.flink.streaming.controlplane.streammanager.abstraction.TriskWithLock;
import org.apache.flink.streaming.controlplane.udm.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author trx StreamManager implementation.
 *     <p>TODO: 1. decide other fields 2. initialize other fields 3. i do not know how to decouple
 *     the connection between stream manager and flink job master
 */
public class StreamManager extends FencedRpcEndpoint<StreamManagerId>
        implements StreamManagerGateway, StreamManagerService, ReconfigurationExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamManager.class);
    /** Default names for Flink's distributed components. */
    public static final String Stream_Manager_NAME = "streammanager";

    private final StreamManagerConfiguration streamManagerConfiguration;

    private final ResourceID resourceId;

    private final JobGraph jobGraph;

    private final ClassLoader userCodeLoader;

    private final Time rpcTimeout;

    private final HighAvailabilityServices highAvailabilityServices;

    private final FatalErrorHandler fatalErrorHandler;

    private final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;

    private final BlobWriter blobWriter;

    private final LibraryCacheManager libraryCacheManager;

    private TriskWithLock trisk;

    private ByteClassLoader byteClassLoader = ByteClassLoader.create();

    private CompletableFuture<Acknowledge> rescalePartitionFuture;

    /*

    // --------- JobManager --------

    private final LeaderRetrievalService jobManagerLeaderRetriever;

    */

    private final JobLeaderIdService jobLeaderIdService;

    private JobManagerRegistration jobManagerRegistration = null;

    private JobID jobId = null;

    public static final String CONTROLLER = "trisk.controller";

    // ------------------------------------------------------------------------

    public StreamManager(
            RpcService rpcService,
            StreamManagerConfiguration streamManagerConfiguration,
            ResourceID resourceId,
            JobGraph jobGraph,
            ClassLoader userCodeLoader,
            HighAvailabilityServices highAvailabilityService,
            JobLeaderIdService jobLeaderIdService,
            LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever,
            LibraryCacheManager libraryCacheManager,
            BlobWriter blobWriter,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        super(rpcService, RpcServiceUtils.createRandomName(Stream_Manager_NAME), null);

        this.streamManagerConfiguration = checkNotNull(streamManagerConfiguration);
        this.resourceId = checkNotNull(resourceId);
        this.jobGraph = checkNotNull(jobGraph);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.rpcTimeout = streamManagerConfiguration.getRpcTimeout();
        this.highAvailabilityServices = checkNotNull(highAvailabilityService);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
        this.dispatcherGatewayRetriever = checkNotNull(dispatcherGatewayRetriever);
        this.blobWriter = blobWriter;
        this.libraryCacheManager = libraryCacheManager;

        final String jobName = jobGraph.getName();
        final JobID jid = jobGraph.getJobID();

        log.debug("Initializing sm for job {} ({})", jobName, jid);
        log.info("Initializing sm for job {} ({})", jobName, jid);
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerJobManager(
            JobMasterId jobMasterId,
            ResourceID jobMasterResourceId,
            String jobMasterAddress,
            JobID jobId,
            Time timeout) {
        return null;
    }

    @Override
    public void disconnectJobManager(JobID jobId, Exception cause) {}

    @Override
    public void streamSwitchCompleted(JobVertexID targetVertexID) {}

    @Override
    public void jobStatusChanged(
            JobID jobId,
            JobStatus newJobStatus,
            long timestamp,
            Throwable error,
            ExecutionPlan jobAbstraction) {}

    @Override
    public ExecutionPlanAndJobGraphUpdaterFactory getStreamRelatedInstanceFactory() {
        return null;
    }

    @Override
    public boolean registerNewController(String controllerID, String className, String sourceCode) {
        return false;
    }

    @Override
    public CompletableFuture<Acknowledge> start(StreamManagerId streamManagerId) throws Exception {
        return null;
    }

    @Override
    public CompletableFuture<Acknowledge> suspend(Exception cause) {
        return null;
    }

    @Override
    public StreamManagerGateway getGateway() {
        return null;
    }

    @Override
    public ExecutionPlan getTrisk() {
        return null;
    }

    @Override
    public TriskWithLock getExecutionPlanCopy() {
        return null;
    }

    @Override
    public void execute(ControlPolicy controller, TriskWithLock executionPlanCopy) {}

    @Override
    public Configuration getExperimentConfig() {
        return null;
    }

    @Override
    public void rescaleStreamJob(JobRescaleAction.RescaleParamsWrapper wrapper) {}

    @Override
    public void rescale(
            int operatorID,
            int newParallelism,
            Map<Integer, List<Integer>> keyStateAllocation,
            ControlPolicy waitingController) {}

    @Override
    public void rescale(
            ExecutionPlan executionPlan,
            int operatorID,
            Boolean isScaleIn,
            ControlPolicy waitingController) {}

    @Override
    public void placement(
            int operatorID,
            Map<Integer, List<Integer>> keyStateAllocation,
            ControlPolicy waitingController) {}

    @Override
    public void rebalance(
            int operatorID,
            Map<Integer, List<Integer>> keyStateAllocation,
            boolean stateful,
            ControlPolicy waitingController) {}

    @Override
    public void rebalance(
            ExecutionPlan executionPlan, int operatorID, ControlPolicy waitingController) {}

    @Override
    public void reconfigureUserFunction(
            int operatorID, Object function, ControlPolicy waitingController) {}

    @Override
    public void noOp(int operatorID, ControlPolicy waitingController) {}

    @Override
    public Class<?> registerFunctionClass(String funcClassName, String sourceCode) {
        return null;
    }
}
