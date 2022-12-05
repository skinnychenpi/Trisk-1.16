package org.apache.flink.streaming.controlplane.entrypoint.streammanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.FileExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.flink.shaded.guava30.com.google.common.base.Ticker;

import java.io.File;
import java.io.IOException;

/** Base class for session cluster entry points. */
public abstract class SessionStreamManagerEntrypoint extends StreamManagerEntrypoint {

    public SessionStreamManagerEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected ExecutionGraphInfoStore createSerializableExecutionGraphStore(
            Configuration configuration, ScheduledExecutor scheduledExecutor) throws IOException {
        final JobManagerOptions.JobStoreType jobStoreType =
                configuration.get(JobManagerOptions.JOB_STORE_TYPE);
        final Time expirationTime =
                Time.seconds(configuration.getLong(JobManagerOptions.JOB_STORE_EXPIRATION_TIME));
        final int maximumCapacity =
                configuration.getInteger(JobManagerOptions.JOB_STORE_MAX_CAPACITY);

        switch (jobStoreType) {
            case File:
            {
                final File tmpDir =
                        new File(ConfigurationUtils.parseTempDirectories(configuration)[0]);
                final long maximumCacheSizeBytes =
                        configuration.getLong(JobManagerOptions.JOB_STORE_CACHE_SIZE);

                return new FileExecutionGraphInfoStore(
                        tmpDir,
                        expirationTime,
                        maximumCapacity,
                        maximumCacheSizeBytes,
                        scheduledExecutor,
                        Ticker.systemTicker());
            }
            case Memory:
            {
                return new MemoryExecutionGraphInfoStore(
                        expirationTime,
                        maximumCapacity,
                        scheduledExecutor,
                        Ticker.systemTicker());
            }
            default:
            {
                throw new IllegalArgumentException(
                        "Unsupported job store type " + jobStoreType);
            }
        }
    }
}
