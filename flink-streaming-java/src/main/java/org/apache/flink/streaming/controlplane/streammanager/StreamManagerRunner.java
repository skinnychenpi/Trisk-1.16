/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.controlplane.streammanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.controlplane.streammanager.StreamManagerGateway;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;

/** Interface for a runner which executes a {@link }. */
public interface StreamManagerRunner extends AutoCloseableAsync {

    /**
     * Start the execution of the {@link }.
     *
     * @throws Exception if the JobMaster cannot be started
     */
    void start() throws Exception;

    /**
     * Get the {@link StreamManagerGateway} of the {@link }. The future is only completed if the
     * JobMaster becomes leader.
     *
     * @return Future with the JobMasterGateway once the underlying JobMaster becomes leader
     */
    CompletableFuture<StreamManagerGateway> getStreamManagerGateway();

    /**
     * Get the job id of the executed job.
     *
     * @return job id of the executed job
     */
    JobID getJobID();
    /**
     * Get the result future of this runner. The future is completed once the executed job reaches a
     * globally terminal state or if the initialization of the {@link JobMaster} fails. If the
     * result future is completed exceptionally via {@link JobNotFinishedException}, then this
     * signals that the job has not been completed successfully. All other exceptional completions
     * denote an unexpected exception which leads to a process restart.
     *
     * @return Future which is completed with the job result
     */
    CompletableFuture<JobManagerRunnerResult> getResultFuture();
}
