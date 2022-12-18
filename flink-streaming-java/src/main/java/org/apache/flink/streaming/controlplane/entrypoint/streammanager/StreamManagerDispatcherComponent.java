package org.apache.flink.streaming.controlplane.entrypoint.streammanager;

import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.streaming.controlplane.dispatcher.runner.StreamManagerDispatcherRunner;
import org.apache.flink.streaming.controlplane.webmonitor.StreamManagerWebMonitorEndpoint;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Component which starts a {@link Dispatcher}, {@link ResourceManager} and {@link WebMonitorEndpoint}
 * in the same process.
 */
public class StreamManagerDispatcherComponent implements AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(StreamManagerDispatcherComponent.class);

    @Nonnull
    private final StreamManagerDispatcherRunner smDispatcherRunner;

    @Nonnull
    private final LeaderRetrievalService smDispatcherLeaderRetrievalService;

    @Nonnull
    private final StreamManagerWebMonitorEndpoint<?> smWebMonitorEndpoint;

    private final CompletableFuture<Void> terminationFuture;

    private final CompletableFuture<ApplicationStatus> shutDownFuture;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    StreamManagerDispatcherComponent(
            @Nonnull StreamManagerDispatcherRunner smDispatcherRunner,
            @Nonnull LeaderRetrievalService smDispatcherLeaderRetrievalService,
            @Nonnull StreamManagerWebMonitorEndpoint<?> smWebMonitorEndpoint) {
        this.smDispatcherRunner = smDispatcherRunner;
        this.smDispatcherLeaderRetrievalService = smDispatcherLeaderRetrievalService;
        this.smWebMonitorEndpoint = smWebMonitorEndpoint;
        this.terminationFuture = new CompletableFuture<>();
        this.shutDownFuture = new CompletableFuture<>();

        registerShutDownFuture();
    }

    private void registerShutDownFuture() {
        FutureUtils.forward(smDispatcherRunner.getShutDownFuture(), shutDownFuture);
    }

    public final CompletableFuture<ApplicationStatus> getShutDownFuture() {
        return shutDownFuture;
    }

    /**
     * Deregister the Flink application from the resource management system by signalling
     * the {@link ResourceManager}.
     *
     * @param applicationStatus to terminate the application with
     * @param diagnostics additional information about the shut down, can be {@code null}
     * @return Future which is completed once the shut down
     */
    public CompletableFuture<Void> deregisterApplicationAndClose(
            final ApplicationStatus applicationStatus,
            final @Nullable String diagnostics) {

        if (isRunning.compareAndSet(true, false)) {
            final CompletableFuture<Void> closeSMWebMonitorAndDeregisterAppFuture = smWebMonitorEndpoint.closeAsync();
//				FutureUtils.composeAfterwards(smWebMonitorEndpoint.closeAsync(), () -> deregisterApplication(applicationStatus, diagnostics));

            return FutureUtils.composeAfterwards(closeSMWebMonitorAndDeregisterAppFuture, this::closeAsyncInternal);
        } else {
            return terminationFuture;
        }
    }

//    private CompletableFuture<Void> closeAsyncInternal() {
//        LOG.info("Closing components.");
//
//        Exception exception = null;
//
//        final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
//
//        try {
//            smDispatcherLeaderRetrievalService.stop();
//        } catch (Exception e) {
//            exception = ExceptionUtils.firstOrSuppressed(e, exception);
//        }
//
//        terminationFutures.add(smDispatcherRunner.closeAsync());
//
//        if (exception != null) {
//            terminationFutures.add(FutureUtils.completedExceptionally(exception));
//        }
//
//        final CompletableFuture<Void> componentTerminationFuture = FutureUtils.completeAll(terminationFutures);
//
//        componentTerminationFuture.whenComplete((aVoid, throwable) -> {
//            if (throwable != null) {
//                terminationFuture.completeExceptionally(throwable);
//            } else {
//                terminationFuture.complete(aVoid);
//            }
//        });
//
//        return terminationFuture;
//    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return stopApplication(ApplicationStatus.CANCELED, "StreamManagerDispatcherComponent has been closed.");
    }

    /**
     * Deregister the Flink application from the resource management system by signalling the {@link
     * ResourceManager} and also stop the process.
     *
     * @param applicationStatus to terminate the application with
     * @param diagnostics additional information about the shut down, can be {@code null}
     * @return Future which is completed once the shut down
     */
    public CompletableFuture<Void> stopApplication(
            final ApplicationStatus applicationStatus, final @Nullable String diagnostics) {
        return internalShutdown(FutureUtils::completedVoidFuture);
    }

    /**
     * Close the web monitor and cluster components. This method will not deregister the Flink
     * application from the resource management and only stop the process.
     *
     * @return Future which is completed once the shut down
     */
    public CompletableFuture<Void> stopProcess() {
        return internalShutdown(FutureUtils::completedVoidFuture);
    }

    private CompletableFuture<Void> internalShutdown(
            final Supplier<CompletableFuture<?>> additionalShutdownAction) {
        if (isRunning.compareAndSet(true, false)) {
//            final CompletableFuture<Void> operationsConsumedFuture =
//                    dispatcherOperationCaches.shutdownCaches();
            final CompletableFuture<Void> webMonitorShutdownFuture = smWebMonitorEndpoint.closeAsync();
//            final CompletableFuture<Void> closeWebMonitorAndAdditionalShutdownActionFuture =
//                    FutureUtils.composeAfterwards(
//                            webMonitorShutdownFuture, additionalShutdownAction);

            return FutureUtils.composeAfterwards(
                    webMonitorShutdownFuture, this::closeAsyncInternal);
        } else {
            return terminationFuture;
        }
    }

    private CompletableFuture<Void> closeAsyncInternal() {
        LOG.info("Closing components.");

        Exception exception = null;

        final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

        try {
            smDispatcherLeaderRetrievalService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

//        try {
//            resourceManagerRetrievalService.stop();
//        } catch (Exception e) {
//            exception = ExceptionUtils.firstOrSuppressed(e, exception);
//        }

        terminationFutures.add(smDispatcherRunner.closeAsync());

//        terminationFutures.add(resourceManagerService.closeAsync());

        if (exception != null) {
            terminationFutures.add(FutureUtils.completedExceptionally(exception));
        }

        final CompletableFuture<Void> componentTerminationFuture =
                FutureUtils.completeAll(terminationFutures);

        componentTerminationFuture.whenComplete(
                (aVoid, throwable) -> {
                    if (throwable != null) {
                        terminationFuture.completeExceptionally(throwable);
                    } else {
                        terminationFuture.complete(aVoid);
                    }
                });

        return terminationFuture;
    }

}
