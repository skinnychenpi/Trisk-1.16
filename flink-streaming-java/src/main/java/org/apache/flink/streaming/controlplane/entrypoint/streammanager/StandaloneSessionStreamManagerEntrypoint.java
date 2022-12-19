package org.apache.flink.streaming.controlplane.entrypoint.streammanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfiguration;
import org.apache.flink.runtime.entrypoint.EntrypointClusterConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

/** Entry point for the standalone session controller cluster. */
public class StandaloneSessionStreamManagerEntrypoint extends SessionStreamManagerEntrypoint {

    public StandaloneSessionStreamManagerEntrypoint(Configuration configuration) {
        super(configuration);
    }

    //    @Override
    //    protected DefaultDispatcherResourceManagerComponentFactory
    //    createDispatcherResourceManagerComponentFactory(Configuration configuration) {
    //        return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
    //                StandaloneResourceManagerFactory.getInstance());
    //    }

    @Override
    protected DefaultStreamManagerDispatcherComponentFactory
            createStreamManagerDispatcherComponentFactory(Configuration configuration) {
        return DefaultStreamManagerDispatcherComponentFactory.createSessionComponentFactory();
    }

    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(
                LOG,
                org.apache.flink.streaming.controlplane.entrypoint
                        .StandaloneSessionClusterControllerEntrypoint.class
                        .getSimpleName(),
                args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        final EntrypointClusterConfiguration entrypointClusterConfiguration =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new EntrypointClusterConfigurationParserFactory(),
                        StandaloneSessionClusterEntrypoint.class);
        Configuration configuration = loadConfiguration(entrypointClusterConfiguration);

        StandaloneSessionStreamManagerEntrypoint entrypoint =
                new StandaloneSessionStreamManagerEntrypoint(configuration);

        StreamManagerEntrypoint.runClusterEntrypoint(entrypoint);
    }
}
