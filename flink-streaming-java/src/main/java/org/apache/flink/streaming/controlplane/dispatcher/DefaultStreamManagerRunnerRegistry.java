package org.apache.flink.streaming.controlplane.dispatcher;


import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@code DefaultStreamManagerRunnerRegistry} is the default implementation of the {@link
 * StreamManagerRunnerRegistry} interface.
 */
public class DefaultStreamManagerRunnerRegistry implements StreamManagerRunnerRegistry {

    @VisibleForTesting final Map<JobID, StreamManagerRunner> streamManagerRunners;

    public DefaultStreamManagerRunnerRegistry(int initialCapacity) {
        Preconditions.checkArgument(initialCapacity > 0);
        streamManagerRunners = new HashMap<>(initialCapacity);
    }

    @Override
    public boolean isRegistered(JobID jobId) {
        return streamManagerRunners.containsKey(jobId);
    }

    @Override
    public void register(StreamManagerRunner jobManagerRunner) {
        Preconditions.checkArgument(
                !isRegistered(jobManagerRunner.getJobID()),
                "A job with the ID %s is already registered.",
                jobManagerRunner.getJobID());
        this.streamManagerRunners.put(jobManagerRunner.getJobID(), jobManagerRunner);
    }

    @Override
    public StreamManagerRunner get(JobID jobId) {
        assertJobRegistered(jobId);
        return this.streamManagerRunners.get(jobId);
    }

    @Override
    public int size() {
        return this.streamManagerRunners.size();
    }

    @Override
    public Set<JobID> getRunningJobIds() {
        return new HashSet<>(this.streamManagerRunners.keySet());
    }

    @Override
    public Collection<StreamManagerRunner> getStreamManagerRunners() {
        return new ArrayList<>(this.streamManagerRunners.values());
    }

    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor unusedExecutor) {
        if (isRegistered(jobId)) {
            try {
                unregister(jobId).close();
            } catch (Exception e) {
                return FutureUtils.completedExceptionally(e);
            }
        }

        return FutureUtils.completedVoidFuture();
    }

    @Override
    public StreamManagerRunner unregister(JobID jobId) {
        assertJobRegistered(jobId);
        return this.streamManagerRunners.remove(jobId);
    }

    private void assertJobRegistered(JobID jobId) {
        if (!isRegistered(jobId)) {
            throw new NoSuchElementException(
                    "There is no running job registered for the job ID " + jobId);
        }
    }
}
