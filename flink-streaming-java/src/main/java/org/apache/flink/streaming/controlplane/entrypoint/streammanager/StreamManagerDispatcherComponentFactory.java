package org.apache.flink.streaming.controlplane.entrypoint.streammanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.Executor;

/**
 * Factory for the {@link DispatcherResourceManagerComponent}.
 */
public interface StreamManagerDispatcherComponentFactory {

    StreamManagerDispatcherComponent create(
            Configuration configuration,
            Executor ioExecutor,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            BlobServer blobServer,
            HeartbeatServices heartbeatServices,
            FatalErrorHandler fatalErrorHandler) throws Exception;
}
