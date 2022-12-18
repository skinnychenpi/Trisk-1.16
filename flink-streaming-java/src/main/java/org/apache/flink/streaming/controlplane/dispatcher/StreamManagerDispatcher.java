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

package org.apache.flink.streaming.controlplane.dispatcher;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleaner;
import org.apache.flink.runtime.entrypoint.ClusterEntryPointExceptionUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.JobResultStoreOptions;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.dispatcher.DispatcherException;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
//import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRunnerImpl;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible
 * for receiving job submissions, persisting them, spawning JobManagers to execute
 * the jobs and to recover them in case of a master failure. Furthermore, it knows
 * about the state of the Flink session cluster.
 */
public abstract class StreamManagerDispatcher extends FencedRpcEndpoint<StreamManagerDispatcherId> implements StreamManagerDispatcherGateway {

    public static final String DISPATCHER_NAME = "smDispatcher";

    private final Configuration configuration;

    private final JobGraphWriter jobGraphWriter;
//    private final RunningJobsRegistry runningJobsRegistry;

    private final OnMainThreadStreamManagerRunnerRegistry streamManagerRunnerRegistry;

    private final JobResultStore jobResultStore;

    private final HighAvailabilityServices highAvailabilityServices;
    private final JobManagerSharedServices jobManagerSharedServices;
    private final BlobServer blobServer;

    private final FatalErrorHandler fatalErrorHandler;

    private final Map<JobID, CompletableFuture<StreamManagerRunner>> streamManagerRunnerFutures;

    private final Collection<JobGraph> recoveredJobs;


    private StreamManagerRunnerFactory streamManagerRunnerFactory;

    private final Map<JobID, CompletableFuture<Void>> streamManagerTerminationFutures;

    protected final CompletableFuture<ApplicationStatus> shutDownFuture;

    private final LeaderRetrievalService dispatcherLeaderRetrievalService;

    private final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;

    private final ResourceCleaner localResourceCleaner;
    private final ResourceCleaner globalResourceCleaner;
    private final Executor ioExecutor;
    private final HistoryServerArchivist historyServerArchivist;
    private final ExecutionGraphInfoStore executionGraphInfoStore;


    public StreamManagerDispatcher(
            RpcService rpcService,
            String endpointId,
            StreamManagerDispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            StreamManagerDispatcherServices dispatcherServices) throws Exception {
        this(rpcService, endpointId, fencingToken, recoveredJobs, dispatcherServices, new DefaultStreamManagerRunnerRegistry(16));

    }
    private StreamManagerDispatcher(
            RpcService rpcService,
            String endpointId,
            StreamManagerDispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            StreamManagerDispatcherServices dispatcherServices,
            StreamManagerRunnerRegistry registry) throws Exception {
        this(rpcService, endpointId, fencingToken, recoveredJobs, dispatcherServices, registry, new StreamManagerDispatcherResourceCleanerFactory(registry, dispatcherServices));

    }
    private StreamManagerDispatcher(
            RpcService rpcService,
            String endpointId,
            StreamManagerDispatcherId fencingToken,
            Collection<JobGraph> recoveredJobs,
            StreamManagerDispatcherServices dispatcherServices,
            StreamManagerRunnerRegistry registry,
            StreamManagerDispatcherResourceCleanerFactory factory) throws Exception {
        super(rpcService, endpointId, fencingToken);
        Preconditions.checkNotNull(dispatcherServices);

        this.configuration = dispatcherServices.getConfiguration();
        this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
        this.blobServer = dispatcherServices.getBlobServer();
        this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
        this.jobGraphWriter = dispatcherServices.getJobGraphWriter();

        this.jobManagerSharedServices = JobManagerSharedServices.fromConfigurationForStreamManager(
                configuration,
                blobServer,
                fatalErrorHandler);

//        this.runningJobsRegistry = highAvailabilityServices.getRunningJobsRegistry();
        this.streamManagerRunnerRegistry = new OnMainThreadStreamManagerRunnerRegistry(registry, this.getMainThreadExecutor());

        streamManagerRunnerFutures = new HashMap<>(16);

        this.streamManagerRunnerFactory = dispatcherServices.getStreamManagerRunnerFactory();

        this.streamManagerTerminationFutures = new HashMap<>(2);

        this.shutDownFuture = new CompletableFuture<>();

        this.recoveredJobs = new HashSet<>(recoveredJobs);

        this.jobResultStore = highAvailabilityServices.getJobResultStore();

        this.dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();
        this.dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
                rpcService,
                DispatcherGateway.class,
                DispatcherId::fromUuid,
                new FixedRetryStrategy(10, Duration.ofMillis(50L)));

        this.globalResourceCleaner = factory.createGlobalResourceCleaner(this.getMainThreadExecutor());

        this.localResourceCleaner = factory.createLocalResourceCleaner(this.getMainThreadExecutor());

        this.ioExecutor = dispatcherServices.getIoExecutor();

        this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

        this.executionGraphInfoStore = dispatcherServices.getArchivedExecutionGraphStore();

    }

    public LeaderGatewayRetriever<DispatcherGateway> getStartedDispatcherRetriever() {
        return this.dispatcherGatewayRetriever;
    }

    //------------------------------------------------------
    // Getters
    //------------------------------------------------------

    public CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    //------------------------------------------------------
    // Lifecycle methods
    //------------------------------------------------------

    @Override
    public void onStart() throws Exception {
        try {
            startDispatcherServices();
        } catch (Exception e) {
            final DispatcherException exception = new DispatcherException(String.format("Could not start the Dispatcher %s", getAddress()), e);
            onFatalError(exception);
            throw exception;
        }

        startRecoveredJobs();
    }

    private void startDispatcherServices() throws Exception {
        try {
            // set up the connection in dispatcherGatewayRetriever.
            dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);
        } catch (Exception e) {
            handleStartDispatcherServicesException(e);
        }
        log.info("start dispatcher services");
    }

    private void startRecoveredJobs() {
        log.info("no op currently");
    }

    private BiFunction<Void, Throwable, Void> handleRecoveredJobStartError(JobID jobId) {
        return (ignored, throwable) -> {
            if (throwable != null) {
                onFatalError(new DispatcherException(String.format("Could not start recovered job %s.", jobId), throwable));
            }

            return null;
        };
    }

    private void handleStartDispatcherServicesException(Exception e) throws Exception {
        try {
            stopDispatcherServices();
        } catch (Exception exception) {
            e.addSuppressed(exception);
        }

        throw e;
    }

    @Override
    public CompletableFuture<Void> onStop() {
        log.info("Stopping dispatcher {}.", getAddress());

        final CompletableFuture<Void> allStreamManagerRunnersTerminationFuture = terminateStreamManagerRunnersAndGetTerminationFuture();

        return FutureUtils.runAfterwards(
                allStreamManagerRunnersTerminationFuture,
                () -> {
                    stopDispatcherServices();

                    log.info("Stopped dispatcher {}.", getAddress());
                });
    }


    private void stopDispatcherServices() throws Exception {
        Exception exception = null;
        try {
            dispatcherLeaderRetrievalService.stop();
            jobManagerSharedServices.shutdown();
        } catch (Exception e) {
            exception = e;
        }

        ExceptionUtils.tryRethrowException(exception);
    }

    //------------------------------------------------------
    // RPCs
    //------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
        log.info(
                "Received JobGraph submission '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());

        try {
            if (isDuplicateJob(jobGraph.getJobID())) {
                if (isInGloballyTerminalState(jobGraph.getJobID())) {
                    log.warn(
                            "Ignoring JobGraph submission '{}' ({}) because the job already reached a globally-terminal state (i.e. {}) in a previous execution.",
                            jobGraph.getName(),
                            jobGraph.getJobID(),
                            Arrays.stream(JobStatus.values())
                                    .filter(JobStatus::isGloballyTerminalState)
                                    .map(JobStatus::name)
                                    .collect(Collectors.joining(", ")));
                }

                final DuplicateJobSubmissionException exception =
                        isInGloballyTerminalState(jobGraph.getJobID())
                                ? DuplicateJobSubmissionException.ofGloballyTerminated(
                                jobGraph.getJobID())
                                : DuplicateJobSubmissionException.of(jobGraph.getJobID());
                return FutureUtils.completedExceptionally(exception);
            } else if (isPartialResourceConfigured(jobGraph)) {
                return FutureUtils.completedExceptionally(
                        new JobSubmissionException(
                                jobGraph.getJobID(),
                                "Currently jobs is not supported if parts of the vertices have "
                                        + "resources configured. The limitation will be removed in future versions."));
            } else {
                return internalSubmitJob(jobGraph);
            }
        } catch (FlinkException e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

//    private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
//        log.info("[SMD] Submitting job {} ({})", jobGraph.getJobID(), jobGraph.getName());
//
//        final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingStreamManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
//                .thenApply(ignored -> Acknowledge.get());
//
//        return persistAndRunFuture.handleAsync((acknowledge, throwable) -> {
//            if (throwable != null) {
//                cleanUpJobData(jobGraph.getJobID(), true);
//
//                final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
//                log.error("sm Failed to submit job {}.", jobGraph.getJobID(), strippedThrowable);
//                throw new CompletionException(
//                        new JobSubmissionException(jobGraph.getJobID(), "Failed to submit job.", strippedThrowable));
//            } else {
//                return acknowledge;
//            }
//        }, getRpcService().getExecutor());
//    }
    CompletableFuture<Void> getStreamManagerTerminationFuture(JobID jobId) {
        if (streamManagerRunnerFutures.containsKey(jobId)) {
            return FutureUtils.completedExceptionally(new DispatcherException(String.format("sm - Job with job id %s is still running.", jobId)));
        } else {
            return streamManagerTerminationFutures.getOrDefault(jobId, CompletableFuture.completedFuture(null));
        }
    }
    private CompletableFuture<Void> waitForTerminatingStreamManager(
            JobID jobId, JobGraph jobGraph, ThrowingConsumer<JobGraph, ?> action) {
        final CompletableFuture<Void> jobManagerTerminationFuture =
                getStreamManagerTerminationFuture(jobId)
                        .exceptionally(
                                (Throwable throwable) -> {
                                    throw new CompletionException(
                                            new DispatcherException(
                                                    String.format(
                                                            "Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.",
                                                            jobId),
                                                    throwable));
                                });

        return jobManagerTerminationFuture.thenAcceptAsync(
                FunctionUtils.uncheckedConsumer(
                        (ignored) -> {
                            streamManagerTerminationFutures.remove(jobId);
                            action.accept(jobGraph);
                        }),
                getMainThreadExecutor());
    }

    private CompletableFuture<Acknowledge> handleTermination(
            JobID jobId, @Nullable Throwable terminationThrowable) {
        if (terminationThrowable != null) {
            return globalResourceCleaner
                    .cleanupAsync(jobId)
                    .handleAsync(
                            (ignored, cleanupThrowable) -> {
                                if (cleanupThrowable != null) {
                                    log.warn(
                                            "Cleanup didn't succeed after job submission failed for job {}.",
                                            jobId,
                                            cleanupThrowable);
                                    terminationThrowable.addSuppressed(cleanupThrowable);
                                }
                                ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(
                                        terminationThrowable);
                                final Throwable strippedThrowable =
                                        ExceptionUtils.stripCompletionException(
                                                terminationThrowable);
                                log.error("Failed to submit job {}.", jobId, strippedThrowable);
                                throw new CompletionException(
                                        new JobSubmissionException(
                                                jobId, "Failed to submit job.", strippedThrowable));
                            },
                            getMainThreadExecutor());
        }
        return CompletableFuture.completedFuture(Acknowledge.get());
    }
    private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
        log.info("Submitting job '{}' ({}).", jobGraph.getName(), jobGraph.getJobID());
        return waitForTerminatingStreamManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
                .handle((ignored, throwable) -> handleTermination(jobGraph.getJobID(), throwable))
                .thenCompose(Function.identity());
    }

//    private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) {
//        final CompletableFuture<Void> runSMFuture = runStreamManager(jobGraph);
//        return runSMFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
//            if (throwable != null) {
//                final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(throwable);
//
//                log.error("Failed to run stream manager.", strippedThrowable);
//                jobGraphWriter.removeJobGraph(jobGraph.getJobID());
//            }
//        }));
//    }

    // Below is Flink 1.16 version.
    private void persistAndRunJob(JobGraph jobGraph) throws Exception {
        jobGraphWriter.putJobGraph(jobGraph);
        runJob(createStreamMasterRunner(jobGraph), ExecutionType.SUBMISSION);
    }

    private StreamManagerRunner createStreamMasterRunner(JobGraph jobGraph) throws Exception {
        Preconditions.checkState(!streamManagerRunnerRegistry.isRegistered(jobGraph.getJobID()));
        return streamManagerRunnerFactory.createStreamManagerRunner(
                jobGraph,
                configuration,
                getRpcService(),
                highAvailabilityServices,
                dispatcherGatewayRetriever,
                jobManagerSharedServices.getLibraryCacheManager(),
                jobManagerSharedServices.getBlobWriter(),
                fatalErrorHandler);
    }

    private void runJob(StreamManagerRunner streamManagerRunner, ExecutionType executionType)
            throws Exception {
        streamManagerRunner.start();
        streamManagerRunnerRegistry.register(streamManagerRunner);

        final JobID jobId = streamManagerRunner.getJobID();

        final CompletableFuture<CleanupJobState> cleanupJobStateFuture =
                streamManagerRunner
                        .getResultFuture()
                        .handleAsync(
                                (jobManagerRunnerResult, throwable) -> {
                                    Preconditions.checkState(
                                            streamManagerRunnerRegistry.isRegistered(jobId)
                                                    && streamManagerRunnerRegistry.get(jobId)
                                                    == streamManagerRunner,
                                            "The job entry in runningJobs must be bound to the lifetime of the StreamManagerRunner.");

                                    if (jobManagerRunnerResult != null) {
                                        return handleJobManagerRunnerResult(
                                                jobManagerRunnerResult, executionType);
                                    } else {
                                        return CompletableFuture.completedFuture(
                                                streamManagerRunnerFailed(
                                                        jobId, JobStatus.FAILED, throwable));
                                    }
                                },
                                getMainThreadExecutor())
                        .thenCompose(Function.identity());

        final CompletableFuture<Void> jobTerminationFuture =
                cleanupJobStateFuture.thenCompose(
                        cleanupJobState ->
                                removeJob(jobId, cleanupJobState)
                                        .exceptionally(
                                                throwable ->
                                                        logCleanupErrorWarning(jobId, throwable)));

        FutureUtils.handleUncaughtException(
                jobTerminationFuture,
                (thread, throwable) -> fatalErrorHandler.onFatalError(throwable));
        registerStreamManagerRunnerTerminationFuture(jobId, jobTerminationFuture);
    }

    // This is Trisk 1.10 code
//    private CompletableFuture<Void> runStreamManager(JobGraph jobGraph) {
//
//        final CompletableFuture<StreamManagerRunner> streamManagerRunnerFuture = createStreamManagerRunner(jobGraph);
//        this.streamManagerRunnerFutures.put(jobGraph.getJobID(), streamManagerRunnerFuture);
//
//        return streamManagerRunnerFuture
//                .thenApply(FunctionUtils.uncheckedFunction(this::startStreamManagerRunner))
//                .thenApply(FunctionUtils.nullFn())
//                .whenCompleteAsync(
//                        (ignored, throwable) -> {
//                        },
//                        getMainThreadExecutor());
//
//    }

    // This is Trisk 1.10 code.
//    private CompletableFuture<StreamManagerRunner> createStreamManagerRunner(JobGraph jobGraph) {
//        final RpcService rpcService = getRpcService();
//        return CompletableFuture.supplyAsync(
//                CheckedSupplier.unchecked(() ->
//                        streamManagerRunnerFactory.createStreamManagerRunner(
//                                jobGraph,
//                                configuration, //configuration,
//                                rpcService,
//                                highAvailabilityServices, //highAvailabilityServices,
//                                dispatcherGatewayRetriever, //heartbeatServices,
//                                jobManagerSharedServices.getLibraryCacheManager(),
//                                jobManagerSharedServices.getBlobWriter(),
//                                fatalErrorHandler //fatalErrorHandler
//                        )),
//                rpcService.getExecutor());
//    }

//    private StreamManagerRunner startStreamManagerRunner(StreamManagerRunner streamManagerRunner) throws Exception {
//        streamManagerRunner.start();
//        return streamManagerRunner;
//    }


//    /**
//     * Checks whether the given job has already been submitted or executed.
//     *
//     * @param jobId identifying the submitted job
//     * @return true if the job has already been submitted (is running) or has been executed
//     * @throws FlinkException if the job scheduling status cannot be retrieved
//     */
//    private boolean isDuplicateJob(JobID jobId) throws FlinkException {
//        final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus;
//
//        try {
//            jobSchedulingStatus = runningJobsRegistry.getJobSchedulingStatus(jobId);
//        } catch (IOException e) {
//            throw new FlinkException(String.format("Failed to retrieve job scheduling status for job %s.", jobId), e);
//        }
//
//        return jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE || streamManagerRunnerFutures.containsKey(jobId);
//    }

    /**
     * Checks whether the given job has already been submitted or executed.
     *
     * @param jobId identifying the submitted job
     * @return true if the job has already been submitted (is running) or has been executed
     * @throws FlinkException if the job scheduling status cannot be retrieved
     */
    private boolean isDuplicateJob(JobID jobId) throws FlinkException {
        return isInGloballyTerminalState(jobId) || streamManagerRunnerRegistry.isRegistered(jobId);
    }

    private boolean isPartialResourceConfigured(JobGraph jobGraph) {
        boolean hasVerticesWithUnknownResource = false;
        boolean hasVerticesWithConfiguredResource = false;

        for (JobVertex jobVertex : jobGraph.getVertices()) {
            if (jobVertex.getMinResources() == ResourceSpec.UNKNOWN) {
                hasVerticesWithUnknownResource = true;
            } else {
                hasVerticesWithConfiguredResource = true;
            }

            if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
                return true;
            }
        }

        return false;
    }


    @Override
    public CompletableFuture<Collection<JobID>> listJobs(Time timeout) {
        return CompletableFuture.completedFuture(
                Collections.unmodifiableSet(new HashSet<>(streamManagerRunnerFutures.keySet())));
    }


    // This is Trisk 1.10 code
//    /**
//     * Cleans up the job related data from the dispatcher. If cleanupHA is true, then
//     * the data will also be removed from HA.
//     *
//     * @param jobId     JobID identifying the job to clean up
//     * @param cleanupHA True iff HA data shall also be cleaned up
//     */
//    private void removeJobAndRegisterTerminationFuture(JobID jobId, boolean cleanupHA) {
//        final CompletableFuture<Void> cleanupFuture = removeJob(jobId, cleanupHA);
//
//        registerStreamManagerRunnerTerminationFuture(jobId, cleanupFuture);
//    }
//
//    private void registerStreamManagerRunnerTerminationFuture(JobID jobId, CompletableFuture<Void> streamManagerRunnerTerminationFuture) {
//        Preconditions.checkState(!streamManagerTerminationFutures.containsKey(jobId));
//
//        streamManagerTerminationFutures.put(jobId, streamManagerRunnerTerminationFuture);
//
//        // clean up the pending termination future
//        streamManagerRunnerTerminationFuture.thenRunAsync(
//                () -> {
//                    final CompletableFuture<Void> terminationFuture = streamManagerTerminationFutures.remove(jobId);
//
//                    //noinspection ObjectEquality
//                    if (terminationFuture != null && terminationFuture != streamManagerRunnerTerminationFuture) {
//                        streamManagerTerminationFutures.put(jobId, terminationFuture);
//                    }
//                },
//                getMainThreadExecutor());
//    }

//    private CompletableFuture<Void> removeJob(JobID jobId, boolean cleanupHA) {
//        CompletableFuture<StreamManagerRunner> streamManagerRunnerFuture = streamManagerRunnerFutures.remove(jobId);
//
//        final CompletableFuture<Void> streamManagerRunnerTerminationFuture;
//        if (streamManagerRunnerFuture != null) {
//            streamManagerRunnerTerminationFuture = streamManagerRunnerFuture.thenCompose(StreamManagerRunner::closeAsync);
//        } else {
//            streamManagerRunnerTerminationFuture = CompletableFuture.completedFuture(null);
//        }
//
//        return streamManagerRunnerTerminationFuture.thenRunAsync(
//                () -> cleanUpJobData(jobId, cleanupHA),
//                getRpcService().getExecutor());
//    }

    // This is the version in FLink 1.16
    private CompletableFuture<Void> removeJob(JobID jobId, CleanupJobState cleanupJobState) {
        if (cleanupJobState.isGlobalCleanup()) {
            return globalResourceCleaner
                    .cleanupAsync(jobId)
                    .thenRunAsync(() -> markJobAsClean(jobId), ioExecutor)
                    .thenRunAsync(
                            () ->
                                    runPostJobGloballyTerminated(
                                            jobId, cleanupJobState.getJobStatus()),
                            getMainThreadExecutor());
        } else {
            return localResourceCleaner.cleanupAsync(jobId);
        }
    }

    private void markJobAsClean(JobID jobId) {
        try {
            jobResultStore.markResultAsClean(jobId);
            log.debug(
                    "Cleanup for the job '{}' has finished. Job has been marked as clean.", jobId);
        } catch (IOException e) {
            log.warn("Could not properly mark job {} result as clean.", jobId, e);
        }
    }

    protected void runPostJobGloballyTerminated(JobID jobId, JobStatus jobStatus) {
        // no-op: we need to provide this method to enable the MiniDispatcher implementation to do
        // stuff after the job is cleaned up
    }

    // This is Trisk 1.10 code
//    private void cleanUpJobData(JobID jobId, boolean cleanupHA) {
//
//        boolean cleanupHABlobs = false;
//        if (cleanupHA) {
//            try {
//                jobGraphWriter.removeJobGraph(jobId);
//
//                // only clean up the HA blobs if we could remove the job from HA storage
//                cleanupHABlobs = true;
//            } catch (Exception e) {
//                log.warn("Could not properly remove job {} from submitted job graph store.", jobId, e);
//            }
//
//            try {
//                runningJobsRegistry.clearJob(jobId);
//            } catch (IOException e) {
//                log.warn("Could not properly remove job {} from the running jobs registry.", jobId, e);
//            }
//        } else {
//            try {
//                jobGraphWriter.releaseJobGraph(jobId);
//            } catch (Exception e) {
//                log.warn("Could not properly release job {} from submitted job graph store.", jobId, e);
//            }
//        }
//
//        blobServer.cleanupJob(jobId, cleanupHABlobs);
//    }


    // This method is modified to adapt new 1.16 version.
    /**
     * Terminate all currently running {@link StreamManagerRunnerImpl}.
     */
    private void terminateStreamManagerRunners() {
        log.info("Stopping all currently running jobs of dispatcher {}.", getAddress());

        final Set<JobID> jobsToRemove = streamManagerRunnerRegistry.getRunningJobIds();

        for (JobID jobId : jobsToRemove) {
            terminateJob(jobId);
        }
    }

    private void terminateJob(JobID jobId) {
        if (streamManagerRunnerRegistry.isRegistered(jobId)) {
            final StreamManagerRunner streamManagerRunner = streamManagerRunnerRegistry.get(jobId);
            streamManagerRunner.closeAsync();
        }
    }

    private CompletableFuture<Void> terminateStreamManagerRunnersAndGetTerminationFuture() {
        terminateStreamManagerRunners();
        final Collection<CompletableFuture<Void>> values = streamManagerTerminationFutures.values();
        return FutureUtils.completeAll(values);
    }

    protected void onFatalError(Throwable throwable) {
        fatalErrorHandler.onFatalError(throwable);
    }

    protected void jobNotFinished(JobID jobId) {
        log.info("Job with ID: {} was not finished by StreamManager.", jobId);
    }

    private void jobMasterFailed(JobID jobId, Throwable cause) {
        // we fail fatally in case of a JobMaster failure in order to restart the
        // dispatcher to recover the jobs again. This only works in HA mode, though
        onFatalError(new FlinkException(String.format("JobMaster for job %s failed.", jobId), cause));
    }


    private CompletableFuture<StreamManagerGateway> getStreamManagerGatewayFuture(JobID jobId) {
        final CompletableFuture<StreamManagerRunner> streamManagerRunnerFuture = streamManagerRunnerFutures.get(jobId);

        if (streamManagerRunnerFuture == null) {
            return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
        } else {
            final CompletableFuture<StreamManagerGateway> leaderGatewayFuture = streamManagerRunnerFuture.thenCompose(StreamManagerRunner::getStreamManagerGateway);
            return leaderGatewayFuture.thenApplyAsync(
                    (StreamManagerGateway streamManagerGateway) -> {
                        // check whether the retrieved JobMasterGateway belongs still to a running JobMaster
                        if (streamManagerRunnerFutures.containsKey(jobId)) {
                            return streamManagerGateway;
                        } else {
                            throw new CompletionException(new FlinkJobNotFoundException(jobId));
                        }
                    },
                    getMainThreadExecutor());
        }
    }

    // Trisk 1.10 code.
//    public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
//        return CompletableFuture.runAsync(
//                () -> removeJobAndRegisterTerminationFuture(jobId, false),
//                getMainThreadExecutor());
//    }

    public CompletableFuture<Void> onRemovedJobGraph(JobID jobId) {
        return CompletableFuture.runAsync(() -> terminateJob(jobId), getMainThreadExecutor());
    }

    @Override
    public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
        return CompletableFuture.completedFuture(blobServer.getPort());
    }

    //Trisk 1.10 Code
//    private CompletableFuture<Void> waitForTerminatingStreamManager(JobID jobId, JobGraph jobGraph, FunctionWithException<JobGraph, CompletableFuture<Void>, ?> action) {
//        final CompletableFuture<Void> streamManagerTerminationFuture = getStreamManagerTerminationFuture(jobId)
//                .exceptionally((Throwable throwable) -> {
//                    throw new CompletionException(
//                            new DispatcherException(
//                                    String.format("Termination of previous StreamManager for job %s failed. Cannot submit job under the same job id.", jobId),
//                                    throwable));
//                });
//
//        return streamManagerTerminationFuture.thenComposeAsync(
//                FunctionUtils.uncheckedFunction((ignored) -> {
//                    streamManagerTerminationFutures.remove(jobId);
//                    return action.apply(jobGraph);
//                }),
//                getMainThreadExecutor());
//    }

    private boolean isInGloballyTerminalState(JobID jobId) throws FlinkException {
        try {
            return jobResultStore.hasJobResultEntry(jobId);
        } catch (IOException e) {
            throw new FlinkException(
                    String.format("Failed to retrieve job scheduling status for job %s.", jobId),
                    e);
        }
    }


//    private CompletableFuture<Void> getStreamManagerTerminationFuture(JobID jobId) {
//        if (streamManagerRunnerFutures.containsKey(jobId)) {
//            return FutureUtils.completedExceptionally(new DispatcherException(String.format("sm - Job with job id %s is still running.", jobId)));
//        } else {
//            return streamManagerTerminationFutures.getOrDefault(jobId, CompletableFuture.completedFuture(null));
//        }
//    }

    private CompletableFuture<CleanupJobState> handleJobManagerRunnerResult(
            JobManagerRunnerResult jobManagerRunnerResult, ExecutionType executionType) {
        if (jobManagerRunnerResult.isInitializationFailure()
                && executionType == ExecutionType.RECOVERY) {
            // fail fatally to make the Dispatcher fail-over and recover all jobs once more (which
            // can only happen in HA mode)
            return CompletableFuture.completedFuture(
                    streamManagerRunnerFailed(
                            jobManagerRunnerResult.getExecutionGraphInfo().getJobId(),
                            JobStatus.INITIALIZING,
                            jobManagerRunnerResult.getInitializationFailure()));
        }
        return jobReachedTerminalState(jobManagerRunnerResult.getExecutionGraphInfo());
    }

    private CleanupJobState streamManagerRunnerFailed(
            JobID jobId, JobStatus jobStatus, Throwable throwable) {
        jobMasterFailed(jobId, throwable);
        return CleanupJobState.localCleanup(jobStatus);
    }

    @VisibleForTesting
    protected CompletableFuture<CleanupJobState> jobReachedTerminalState(
            ExecutionGraphInfo executionGraphInfo) {
        final ArchivedExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();
        final JobStatus terminalJobStatus = archivedExecutionGraph.getState();
        Preconditions.checkArgument(
                terminalJobStatus.isTerminalState(),
                "Job %s is in state %s which is not terminal.",
                archivedExecutionGraph.getJobID(),
                terminalJobStatus);

        // the failureInfo contains the reason for why job was failed/suspended, but for
        // finished/canceled jobs it may contain the last cause of a restart (if there were any)
        // for finished/canceled jobs we don't want to print it because it is misleading
        final boolean isFailureInfoRelatedToJobTermination =
                terminalJobStatus == JobStatus.SUSPENDED || terminalJobStatus == JobStatus.FAILED;

        if (archivedExecutionGraph.getFailureInfo() != null
                && isFailureInfoRelatedToJobTermination) {
            log.info(
                    "Job {} reached terminal state {}.\n{}",
                    archivedExecutionGraph.getJobID(),
                    terminalJobStatus,
                    archivedExecutionGraph.getFailureInfo().getExceptionAsString().trim());
        } else {
            log.info(
                    "Job {} reached terminal state {}.",
                    archivedExecutionGraph.getJobID(),
                    terminalJobStatus);
        }

        writeToExecutionGraphInfoStore(executionGraphInfo);

        if (!terminalJobStatus.isGloballyTerminalState()) {
            return CompletableFuture.completedFuture(
                    CleanupJobState.localCleanup(terminalJobStatus));
        }

        // do not create an archive for suspended jobs, as this would eventually lead to
        // multiple archive attempts which we currently do not support
        CompletableFuture<Acknowledge> archiveFuture =
                archiveExecutionGraphToHistoryServer(executionGraphInfo);

        return archiveFuture.thenCompose(
                ignored -> registerGloballyTerminatedJobInJobResultStore(executionGraphInfo));
    }

    private CompletableFuture<Acknowledge> archiveExecutionGraphToHistoryServer(
            ExecutionGraphInfo executionGraphInfo) {

        return historyServerArchivist
                .archiveExecutionGraph(executionGraphInfo)
                .handleAsync(
                        (Acknowledge ignored, Throwable throwable) -> {
                            if (throwable != null) {
                                log.info(
                                        "Could not archive completed job {}({}) to the history server.",
                                        executionGraphInfo.getArchivedExecutionGraph().getJobName(),
                                        executionGraphInfo.getArchivedExecutionGraph().getJobID(),
                                        throwable);
                            }
                            return Acknowledge.get();
                        },
                        getMainThreadExecutor());
    }

    @Nullable
    private Void logCleanupErrorWarning(JobID jobId, Throwable cleanupError) {
        log.warn(
                "The cleanup of job {} failed. The job's artifacts in the different directories ('{}', '{}', '{}') and its JobResultStore entry in '{}' (in HA mode) should be checked for manual cleanup.",
                jobId,
                configuration.get(HighAvailabilityOptions.HA_STORAGE_PATH),
                configuration.get(BlobServerOptions.STORAGE_DIRECTORY),
                configuration.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY),
                configuration.get(JobResultStoreOptions.STORAGE_PATH),
                cleanupError);
        return null;
    }

    private void registerStreamManagerRunnerTerminationFuture(
            JobID jobId, CompletableFuture<Void> streamManagerRunnerTerminationFuture) {
        Preconditions.checkState(!streamManagerTerminationFutures.containsKey(jobId));
        streamManagerTerminationFutures.put(jobId, streamManagerRunnerTerminationFuture);

        // clean up the pending termination future
        streamManagerRunnerTerminationFuture.thenRunAsync(
                () -> {
                    final CompletableFuture<Void> terminationFuture =
                            streamManagerTerminationFutures.remove(jobId);

                    //noinspection ObjectEquality
                    if (terminationFuture != null
                            && terminationFuture != streamManagerRunnerTerminationFuture) {
                        streamManagerTerminationFutures.put(jobId, terminationFuture);
                    }
                },
                getMainThreadExecutor());
    }

    private CompletableFuture<CleanupJobState> registerGloballyTerminatedJobInJobResultStore(
            ExecutionGraphInfo executionGraphInfo) {
        final CompletableFuture<Void> writeFuture = new CompletableFuture<>();
        final JobID jobId = executionGraphInfo.getJobId();

        final ArchivedExecutionGraph archivedExecutionGraph =
                executionGraphInfo.getArchivedExecutionGraph();

        final JobStatus terminalJobStatus = archivedExecutionGraph.getState();
        Preconditions.checkArgument(
                terminalJobStatus.isGloballyTerminalState(),
                "Job %s is in state %s which is not globally terminal.",
                jobId,
                terminalJobStatus);

        ioExecutor.execute(
                () -> {
                    try {
                        if (jobResultStore.hasCleanJobResultEntry(jobId)) {
                            log.warn(
                                    "Job {} is already marked as clean but clean up was triggered again.",
                                    jobId);
                        } else if (!jobResultStore.hasDirtyJobResultEntry(jobId)) {
                            jobResultStore.createDirtyResult(
                                    new JobResultEntry(
                                            JobResult.createFrom(archivedExecutionGraph)));
                            log.info(
                                    "Job {} has been registered for cleanup in the JobResultStore after reaching a terminal state.",
                                    jobId);
                        }
                    } catch (IOException e) {
                        writeFuture.completeExceptionally(e);
                        return;
                    }
                    writeFuture.complete(null);
                });

        return writeFuture.handleAsync(
                (ignored, error) -> {
                    if (error != null) {
                        fatalErrorHandler.onFatalError(
                                new FlinkException(
                                        String.format(
                                                "The job %s couldn't be marked as pre-cleanup finished in JobResultStore.",
                                                executionGraphInfo.getJobId()),
                                        error));
                    }
                    return CleanupJobState.globalCleanup(terminalJobStatus);
                },
                getMainThreadExecutor());
    }

    private void writeToExecutionGraphInfoStore(ExecutionGraphInfo executionGraphInfo) {
        try {
            executionGraphInfoStore.put(executionGraphInfo);
        } catch (IOException e) {
            log.info(
                    "Could not store completed job {}({}).",
                    executionGraphInfo.getArchivedExecutionGraph().getJobName(),
                    executionGraphInfo.getArchivedExecutionGraph().getJobID(),
                    e);
        }
    }
    /**
     * Requests the {@link JobResult} of a job specified by the given jobId.
     *
     * @param jobId   identifying the job for which to retrieve the {@link JobResult}.
     * @param timeout for the asynchronous operation
     * @return Future which is completed with the job's {@link JobResult} once the job has finished
     */
    public CompletableFuture<JobResult> requestJobResult(JobID jobId, @RpcTimeout Time timeout) {
        Optional<DispatcherGateway> dispatcherGateway = dispatcherGatewayRetriever.getNow();
        return dispatcherGateway.map(gateway -> gateway.requestJobResult(jobId, timeout)).orElse(null);
    }


    public CompletableFuture<JobStatus> requestJobStatus(
            JobID jobId,
            @RpcTimeout Time timeout) {
        Optional<DispatcherGateway> dispatcherGateway = dispatcherGatewayRetriever.getNow();
        return dispatcherGateway.map(gateway -> gateway.requestJobStatus(jobId, timeout)).orElse(null);
    }

    public CompletableFuture<Boolean> registerNewController(
            JobID jobId,
            String controllerID,
            String className,
            String sourceCode,
            @RpcTimeout Time timeout) {

        log.info("register new controller, controllerID:" + controllerID + " class name:" + className + " for job:" + jobId);
        CompletableFuture<StreamManagerGateway> gatewayFuture = getStreamManagerGatewayFuture(jobId);
        CompletableFuture<Boolean> registerFuture = gatewayFuture.thenApply(gateway ->
                gateway.registerNewController(controllerID, className, sourceCode));
        return registerFuture.exceptionally(throwable -> Boolean.FALSE);
    }

    protected enum ExecutionType {
        SUBMISSION,
        RECOVERY
    }

    private static class CleanupJobState {

        private final boolean globalCleanup;
        private final JobStatus jobStatus;

        public static CleanupJobState localCleanup(JobStatus jobStatus) {
            return new CleanupJobState(false, jobStatus);
        }

        public static CleanupJobState globalCleanup(JobStatus jobStatus) {
            return new CleanupJobState(true, jobStatus);
        }

        private CleanupJobState(boolean globalCleanup, JobStatus jobStatus) {
            this.globalCleanup = globalCleanup;
            this.jobStatus = jobStatus;
        }

        public boolean isGlobalCleanup() {
            return globalCleanup;
        }

        public JobStatus getJobStatus() {
            return jobStatus;
        }
    }
}
