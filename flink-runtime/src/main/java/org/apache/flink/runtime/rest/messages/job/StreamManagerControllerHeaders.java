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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.FileUploadHandler;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.*;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;

/** These headers define the protocol for submitting a job to a flink cluster. */
public class StreamManagerControllerHeaders
        implements MessageHeaders<
                SubmitControllerRequestBody, EmptyResponseBody, JobMessageParameters> {

    private static final String URL = "/jobs/:" + JobIDPathParameter.KEY + "/smcontroller";
    private static final StreamManagerControllerHeaders INSTANCE =
            new StreamManagerControllerHeaders();

    private StreamManagerControllerHeaders() {}

    @Override
    public Class<SubmitControllerRequestBody> getRequestClass() {
        return SubmitControllerRequestBody.class;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    // This is a dummy method created compared with Trisk on Flink 1.10.
    @Override
    public Collection<? extends RestAPIVersion<?>> getSupportedAPIVersions() {
        return null;
    }

    @Override
    public Class<EmptyResponseBody> getResponseClass() {
        return EmptyResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.ACCEPTED;
    }

    @Override
    public JobMessageParameters getUnresolvedMessageParameters() {
        return new JobMessageParameters();
    }

    public static StreamManagerControllerHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Submits a job. This call is primarily intended to be used by the Flink client. This call expects a "
                + "multipart/form-data request that consists of file uploads for the serialized JobGraph, jars and "
                + "distributed cache artifacts and an attribute named \""
                + FileUploadHandler.HTTP_ATTRIBUTE_REQUEST
                + "\" for "
                + "the JSON payload.";
    }

    @Override
    public boolean acceptsFileUploads() {
        return true;
    }
}
