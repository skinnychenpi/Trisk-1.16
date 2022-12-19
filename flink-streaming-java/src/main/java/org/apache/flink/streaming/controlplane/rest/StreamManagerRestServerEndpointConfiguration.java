package org.apache.flink.streaming.controlplane.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StreamManagerRestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/** A configuration object for {@link RestServerEndpoint}s. TODO: not sure if we need to use this */
public final class StreamManagerRestServerEndpointConfiguration {

    private final String restAddress;

    @Nullable private final String restBindAddress;

    private final String restBindPortRange;

    @Nullable private final SSLHandlerFactory sslHandlerFactory;

    private final Path uploadDir;

    private final int maxContentLength;

    private final Map<String, String> responseHeaders;

    private StreamManagerRestServerEndpointConfiguration(
            final String restAddress,
            @Nullable String restBindAddress,
            String restBindPortRange,
            @Nullable SSLHandlerFactory sslHandlerFactory,
            final Path uploadDir,
            final int maxContentLength,
            final Map<String, String> responseHeaders) {

        Preconditions.checkArgument(
                maxContentLength > 0,
                "maxContentLength must be positive, was: %s",
                maxContentLength);

        this.restAddress = requireNonNull(restAddress);
        this.restBindAddress = restBindAddress;
        this.restBindPortRange = requireNonNull(restBindPortRange);
        this.sslHandlerFactory = sslHandlerFactory;
        this.uploadDir = requireNonNull(uploadDir);
        this.maxContentLength = maxContentLength;
        this.responseHeaders = Collections.unmodifiableMap(requireNonNull(responseHeaders));
    }

    /** @see RestOptions#ADDRESS */
    public String getRestAddress() {
        return restAddress;
    }

    /**
     * Returns the address that the REST server endpoint should bind itself to.
     *
     * @return address that the REST server endpoint should bind itself to
     */
    public String getRestBindAddress() {
        return restBindAddress;
    }

    /**
     * Returns the port range that the REST server endpoint should listen on.
     *
     * @return port range that the REST server endpoint should listen on
     */
    public String getRestBindPortRange() {
        return restBindPortRange;
    }

    /**
     * Returns the {@link SSLEngine} that the REST server endpoint should use.
     *
     * @return SSLEngine that the REST server endpoint should use, or null if SSL was disabled
     */
    @Nullable
    public SSLHandlerFactory getSslHandlerFactory() {
        return sslHandlerFactory;
    }

    /** Returns the directory used to temporarily store multipart/form-data uploads. */
    public Path getUploadDir() {
        return uploadDir;
    }

    /**
     * Returns the max content length that the REST server endpoint could handle.
     *
     * @return max content length that the REST server endpoint could handle
     */
    public int getMaxContentLength() {
        return maxContentLength;
    }

    /** Response headers that should be added to every HTTP response. */
    public Map<String, String> getResponseHeaders() {
        return responseHeaders;
    }

    /**
     * Creates and returns a new {@link StreamManagerRestServerEndpointConfiguration} from the given
     * {@link Configuration}.
     *
     * @param config configuration from which the REST server endpoint configuration should be
     *     created from
     * @return REST server endpoint configuration
     * @throws ConfigurationException if SSL was configured incorrectly
     */
    public static StreamManagerRestServerEndpointConfiguration fromConfiguration(
            Configuration config) throws ConfigurationException {
        Preconditions.checkNotNull(config);

        final String restAddress =
                Preconditions.checkNotNull(
                        config.getString(StreamManagerRestOptions.ADDRESS),
                        "%s must be set",
                        StreamManagerRestOptions.ADDRESS.key());

        final String restBindAddress = config.getString(StreamManagerRestOptions.BIND_ADDRESS);
        final String portRangeDefinition = config.getString(StreamManagerRestOptions.BIND_PORT);

        final SSLHandlerFactory sslHandlerFactory;
        if (SSLUtils.isRestSSLEnabled(config)) {
            try {
                sslHandlerFactory = SSLUtils.createRestServerSSLEngineFactory(config);
            } catch (Exception e) {
                throw new ConfigurationException(
                        "Failed to initialize SSLEngineFactory for REST server endpoint.", e);
            }
        } else {
            sslHandlerFactory = null;
        }

        final Path uploadDir =
                Paths.get(
                        config.getString(
                                WebOptions.UPLOAD_DIR, config.getString(WebOptions.TMP_DIR)),
                        "flink-web-upload");

        final int maxContentLength =
                config.getInteger(StreamManagerRestOptions.SERVER_MAX_CONTENT_LENGTH);

        final Map<String, String> responseHeaders =
                Collections.singletonMap(
                        HttpHeaders.Names.ACCESS_CONTROL_ALLOW_ORIGIN,
                        config.getString(WebOptions.ACCESS_CONTROL_ALLOW_ORIGIN));

        return new StreamManagerRestServerEndpointConfiguration(
                restAddress,
                restBindAddress,
                portRangeDefinition,
                sslHandlerFactory,
                uploadDir,
                maxContentLength,
                responseHeaders);
    }
}
