package org.apache.flink.streaming.controlplane.dispatcher;

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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerRegistry;
import org.apache.flink.runtime.dispatcher.cleanup.CleanupRetryStrategyFactory;
import org.apache.flink.runtime.dispatcher.cleanup.DefaultResourceCleaner;
import org.apache.flink.runtime.dispatcher.cleanup.GloballyCleanableResource;
import org.apache.flink.runtime.dispatcher.cleanup.LocallyCleanableResource;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleaner;
import org.apache.flink.runtime.dispatcher.cleanup.ResourceCleanerFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.RetryStrategy;

import java.util.concurrent.Executor;

/**
 * {@code DispatcherResourceCleanerFactory} instantiates {@link ResourceCleaner} instances that
 * clean cleanable resources from the {@link org.apache.flink.runtime.dispatcher.Dispatcher}.
 *
 * <p>We need to handle the {@link JobManagerRunnerRegistry} differently due to a dependency between
 * closing the {@link org.apache.flink.runtime.jobmaster.JobManagerRunner} and the {@link
 * HighAvailabilityServices}. This is fixed in {@code FLINK-24038} using a feature flag to
 * enable/disable single leader election for all the {@code JobManager} components. We can remove
 * the priority cleanup logic after removing the per-component leader election.
 */
public class StreamManagerDispatcherResourceCleanerFactory implements ResourceCleanerFactory {

    private static final String STREAM_MANAGER_RUNNER_REGISTRY_LABEL = "StreamManagerRunnerRegistry";
    private static final String JOB_GRAPH_STORE_LABEL = "JobGraphStore";
    private static final String BLOB_SERVER_LABEL = "BlobServer";
    private static final String HA_SERVICES_LABEL = "HighAvailabilityServices";
    private static final String JOB_MANAGER_METRIC_GROUP_LABEL = "JobManagerMetricGroup";

    private final Executor cleanupExecutor;
    private final RetryStrategy retryStrategy;

    private final StreamManagerRunnerRegistry streamManagerRunnerRegistry;
    private final JobGraphWriter jobGraphWriter;
    private final BlobServer blobServer;
    private final HighAvailabilityServices highAvailabilityServices;

    public StreamManagerDispatcherResourceCleanerFactory(
            StreamManagerRunnerRegistry streamManagerRunnerRegistry,
            StreamManagerDispatcherServices dispatcherServices) {
        this(
                dispatcherServices.getIoExecutor(),
                CleanupRetryStrategyFactory.INSTANCE.createRetryStrategy(
                        dispatcherServices.getConfiguration()),
                streamManagerRunnerRegistry,
                dispatcherServices.getJobGraphWriter(),
                dispatcherServices.getBlobServer(),
                dispatcherServices.getHighAvailabilityServices());
    }

    @VisibleForTesting
    public StreamManagerDispatcherResourceCleanerFactory(
            Executor cleanupExecutor,
            RetryStrategy retryStrategy,
            StreamManagerRunnerRegistry streamManagerRunnerRegistry,
            JobGraphWriter jobGraphWriter,
            BlobServer blobServer,
            HighAvailabilityServices highAvailabilityServices) {
        this.cleanupExecutor = Preconditions.checkNotNull(cleanupExecutor);
        this.retryStrategy = retryStrategy;
        this.streamManagerRunnerRegistry = Preconditions.checkNotNull(streamManagerRunnerRegistry);
        this.jobGraphWriter = Preconditions.checkNotNull(jobGraphWriter);
        this.blobServer = Preconditions.checkNotNull(blobServer);
        this.highAvailabilityServices = Preconditions.checkNotNull(highAvailabilityServices);
    }

    @Override
    public ResourceCleaner createLocalResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return DefaultResourceCleaner.forLocallyCleanableResources(
                        mainThreadExecutor, cleanupExecutor, retryStrategy)
                .withPrioritizedCleanup(
                        STREAM_MANAGER_RUNNER_REGISTRY_LABEL,
                        streamManagerRunnerRegistry)
                .withRegularCleanup(JOB_GRAPH_STORE_LABEL, jobGraphWriter)
                .withRegularCleanup(BLOB_SERVER_LABEL, blobServer)
                .build();
    }

    @Override
    public ResourceCleaner createGlobalResourceCleaner(
            ComponentMainThreadExecutor mainThreadExecutor) {
        return DefaultResourceCleaner.forGloballyCleanableResources(
                        mainThreadExecutor, cleanupExecutor, retryStrategy)
                .withPrioritizedCleanup(
                        STREAM_MANAGER_RUNNER_REGISTRY_LABEL,
                        ofLocalResource(streamManagerRunnerRegistry))
                .withRegularCleanup(JOB_GRAPH_STORE_LABEL, jobGraphWriter)
                .withRegularCleanup(BLOB_SERVER_LABEL, blobServer)
                .withRegularCleanup(HA_SERVICES_LABEL, highAvailabilityServices)
                .build();
    }

    /**
     * A simple wrapper for the resources that don't have any artifacts that can outlive the {@link
     * org.apache.flink.runtime.dispatcher.Dispatcher}, but we still want to clean up their local
     * state when we terminate globally.
     *
     * @param localResource Local resource that we want to clean during a global cleanup.
     * @return Globally cleanable resource.
     */
    private static GloballyCleanableResource ofLocalResource(
            LocallyCleanableResource localResource) {
        return localResource::localCleanupAsync;
    }
}
