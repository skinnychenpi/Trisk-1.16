package org.apache.flink.streaming.controlplane.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.DefaultExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherGateway;
import org.apache.flink.streaming.controlplane.webmonitor.StreamManagerRestfulGateway;
import org.apache.flink.streaming.controlplane.webmonitor.StreamManagerWebMonitorEndpoint;

import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link StreamManagerWebMonitorEndpoint} factory.
 *
 * @param <T> type of the {@link StreamManagerRestfulGateway}
 */
public interface StreamManagerRestEndpointFactory<T extends StreamManagerRestfulGateway> {

    StreamManagerWebMonitorEndpoint<T> createRestEndpoint(
            Configuration configuration,
            LeaderGatewayRetriever<StreamManagerDispatcherGateway> dispatcherGatewayRetriever,
            TransientBlobService transientBlobService,
            ScheduledExecutorService executor,
            LeaderElectionService leaderElectionService,
            FatalErrorHandler fatalErrorHandler)
            throws Exception;

    static ExecutionGraphCache createExecutionGraphCache(
            RestHandlerConfiguration restConfiguration) {
        return new DefaultExecutionGraphCache(
                restConfiguration.getTimeout(),
                Time.milliseconds(restConfiguration.getRefreshInterval()));
    }
}
