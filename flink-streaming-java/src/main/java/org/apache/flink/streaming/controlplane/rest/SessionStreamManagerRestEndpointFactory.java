package org.apache.flink.streaming.controlplane.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherGateway;
import org.apache.flink.streaming.controlplane.dispatcher.StreamManagerDispatcherRestEndpoint;
import org.apache.flink.streaming.controlplane.webmonitor.StreamManagerWebMonitorEndpoint;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;

import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link RestEndpointFactory} which creates a {@link DispatcherRestEndpoint}.
 */
public enum SessionStreamManagerRestEndpointFactory implements StreamManagerRestEndpointFactory<StreamManagerDispatcherGateway> {
    INSTANCE;

    @Override
    public StreamManagerWebMonitorEndpoint<StreamManagerDispatcherGateway> createRestEndpoint(
            Configuration configuration,
            LeaderGatewayRetriever<StreamManagerDispatcherGateway> dispatcherGatewayRetriever,
            TransientBlobService transientBlobService,
            ScheduledExecutorService executor,
            LeaderElectionService leaderElectionService,
            FatalErrorHandler fatalErrorHandler) throws Exception {
        final RestHandlerConfiguration restHandlerConfiguration = RestHandlerConfiguration.fromConfiguration(configuration);

        return new StreamManagerDispatcherRestEndpoint(
                RestServerEndpointConfiguration.fromConfigurationForSm(configuration),
                dispatcherGatewayRetriever,
                configuration,
                restHandlerConfiguration,
                executor,
                leaderElectionService,
                RestEndpointFactory.createExecutionGraphCache(restHandlerConfiguration),
                fatalErrorHandler);
    }
}
