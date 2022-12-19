package org.apache.flink.streaming.controlplane.rest.handler;

import org.apache.flink.runtime.rest.handler.AbstractHandler;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;

/**
 * Tracks in-flight client requests.
 *
 * @see AbstractHandler
 */
@ThreadSafe
class InFlightRequestTracker {

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final Phaser phaser =
            new Phaser(1) {
                @Override
                protected boolean onAdvance(final int phase, final int registeredParties) {
                    terminationFuture.complete(null);
                    return true;
                }
            };

    /**
     * Registers an in-flight request.
     *
     * @return {@code true} if the request could be registered; {@code false} if the tracker has
     *     already been terminated.
     */
    public boolean registerRequest() {
        return phaser.register() >= 0;
    }

    /** Deregisters an in-flight request. */
    public void deregisterRequest() {
        phaser.arriveAndDeregister();
    }

    /**
     * Returns a future that completes when the in-flight requests that were registered prior to
     * calling this method are deregistered.
     */
    public CompletableFuture<Void> awaitAsync() {
        phaser.arriveAndDeregister();
        return terminationFuture;
    }
}
