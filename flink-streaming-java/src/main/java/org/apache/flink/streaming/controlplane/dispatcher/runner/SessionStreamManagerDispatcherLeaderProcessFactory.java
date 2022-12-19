/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.controlplane.dispatcher.runner;

import org.apache.flink.runtime.dispatcher.runner.SessionDispatcherLeaderProcess;
import org.apache.flink.runtime.jobmanager.JobPersistenceComponentFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import java.util.UUID;
import java.util.concurrent.Executor;

/** Factory for the {@link SessionDispatcherLeaderProcess}. */
class SessionStreamManagerDispatcherLeaderProcessFactory
        implements StreamManagerDispatcherLeaderProcessFactory {

    private final AbstractStreamManagerDispatcherLeaderProcess
                    .StreamManagerDispatcherGatewayServiceFactory
            dispatcherGatewayServiceFactory;
    private final JobPersistenceComponentFactory jobPersistenceComponentFactory;
    private final Executor ioExecutor;
    private final FatalErrorHandler fatalErrorHandler;

    SessionStreamManagerDispatcherLeaderProcessFactory(
            AbstractStreamManagerDispatcherLeaderProcess
                            .StreamManagerDispatcherGatewayServiceFactory
                    dispatcherGatewayServiceFactory,
            JobPersistenceComponentFactory jobPersistenceComponentFactory,
            Executor ioExecutor,
            FatalErrorHandler fatalErrorHandler) {
        this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
        this.jobPersistenceComponentFactory = jobPersistenceComponentFactory;
        this.ioExecutor = ioExecutor;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public StreamManagerDispatcherLeaderProcess create(UUID leaderSessionID) {
        return SessionStreamManagerDispatcherLeaderProcess.create(
                leaderSessionID,
                dispatcherGatewayServiceFactory,
                jobPersistenceComponentFactory.createJobGraphStore(),
                ioExecutor,
                fatalErrorHandler);
    }
}
