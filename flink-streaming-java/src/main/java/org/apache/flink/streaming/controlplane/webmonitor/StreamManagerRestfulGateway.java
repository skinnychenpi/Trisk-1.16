package org.apache.flink.streaming.controlplane.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.concurrent.CompletableFuture;

/**
 * Gateway for StreamManager restful endpoints.
 *
 * <p>Gateways which implement this method run a REST endpoint which is reachable
 * under the returned address.
 */
public interface StreamManagerRestfulGateway extends RpcGateway {
    /**
     * Requests the {@link JobResult} of a job specified by the given jobId.
     *
     * @param jobId identifying the job for which to retrieve the {@link JobResult}.
     * @param timeout for the asynchronous operation
     * @return Future which is completed with the job's {@link JobResult} once the job has finished
     */
    CompletableFuture<JobResult> requestJobResult(JobID jobId, @RpcTimeout Time timeout);


    default CompletableFuture<Boolean> registerNewController(
            JobID jobId,
            String controllerID,
            String className,
            String sourceCode,
            @RpcTimeout Time timeout) {
        throw new UnsupportedOperationException();
    }

    /**
     * Request the {@link JobStatus} of the given job.
     *
     * @param jobId identifying the job for which to retrieve the JobStatus
     * @param timeout for the asynchronous operation
     * @return A future to the {@link JobStatus} of the given job
     */
    default CompletableFuture<JobStatus> requestJobStatus(
            JobID jobId,
            @RpcTimeout Time timeout) {
        throw new UnsupportedOperationException();
    }
}

