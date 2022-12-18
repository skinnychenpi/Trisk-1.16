package org.apache.flink.streaming.controlplane.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRunner;
import org.apache.flink.util.WrappingProxy;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * {@code OnMainThreadStreamManagerRunnerRegistry} implements {@link StreamManagerRunnerRegistry} guarding
 * the passed {@code StreamManagerRunnerRegistry} instance in a way that it only allows modifying
 * methods to be executed on the component's main thread.
 *
 * @see ComponentMainThreadExecutor
 */
public class OnMainThreadStreamManagerRunnerRegistry
        implements StreamManagerRunnerRegistry, WrappingProxy<StreamManagerRunnerRegistry> {

    private final StreamManagerRunnerRegistry delegate;
    private final ComponentMainThreadExecutor mainThreadExecutor;

    public OnMainThreadStreamManagerRunnerRegistry(
            StreamManagerRunnerRegistry delegate, ComponentMainThreadExecutor mainThreadExecutor) {
        this.delegate = delegate;
        this.mainThreadExecutor = mainThreadExecutor;
    }

    @Override
    public boolean isRegistered(JobID jobId) {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.isRegistered(jobId);
    }

    @Override
    public void register(StreamManagerRunner streamManagerRunner) {
        mainThreadExecutor.assertRunningInMainThread();
        delegate.register(streamManagerRunner);
    }

    @Override
    public StreamManagerRunner get(JobID jobId) {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.get(jobId);
    }

    @Override
    public int size() {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.size();
    }

    @Override
    public Set<JobID> getRunningJobIds() {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.getRunningJobIds();
    }

    @Override
    public Collection<StreamManagerRunner> getStreamManagerRunners() {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.getStreamManagerRunners();
    }

    @Override
    public CompletableFuture<Void> localCleanupAsync(JobID jobId, Executor executor) {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.localCleanupAsync(jobId, executor);
    }

    @Override
    public StreamManagerRunner unregister(JobID jobId) {
        mainThreadExecutor.assertRunningInMainThread();
        return delegate.unregister(jobId);
    }

    /**
     * Returns the delegated {@link StreamManagerRunnerRegistry}. This method can be used to workaround
     * the main thread safeguard.
     */
    @Override
    public StreamManagerRunnerRegistry getWrappedDelegate() {
        return this.delegate;
    }
}

