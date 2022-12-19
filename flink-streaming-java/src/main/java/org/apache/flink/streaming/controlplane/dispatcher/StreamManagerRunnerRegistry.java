package org.apache.flink.streaming.controlplane.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.cleanup.LocallyCleanableResource;
import org.apache.flink.streaming.controlplane.streammanager.StreamManagerRunner;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Set;

/** {@code StreamManagerRunner} collects running jobs represented by {@link StreamManagerRunner}. */
public interface StreamManagerRunnerRegistry extends LocallyCleanableResource {

    /**
     * Checks whether a {@link StreamManagerRunner} is registered under the given {@link JobID}.
     *
     * @param jobId The {@code JobID} to check.
     * @return {@code true}, if a {@code StreamManagerRunner} is registered; {@code false}
     *     otherwise.
     */
    boolean isRegistered(JobID jobId);

    /** Registers the given {@link StreamManagerRunner} instance. */
    void register(StreamManagerRunner jobManagerRunner);

    /**
     * Returns the {@link StreamManagerRunner} for the given {@code JobID}.
     *
     * @throws NoSuchElementException if the passed {@code JobID} does not belong to a registered
     *     {@code StreamManagerRunner}.
     * @see #isRegistered(JobID)
     */
    StreamManagerRunner get(JobID jobId);

    /** Returns the number of {@link StreamManagerRunner} instances currently being registered. */
    int size();

    /** Returns {@link JobID} instances of registered {@link StreamManagerRunner} instances. */
    Set<JobID> getRunningJobIds();

    /** Returns the registered {@link StreamManagerRunner} instances. */
    Collection<StreamManagerRunner> getStreamManagerRunners();

    /**
     * Unregistered the {@link StreamManagerRunner} with the given {@code JobID}. {@code null} is
     * returned if there's no {@code StreamManagerRunner} registered for the given {@link JobID}.
     */
    StreamManagerRunner unregister(JobID jobId);
}
