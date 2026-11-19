/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.cloud.clients.aws.s3;

import org.apache.asterix.cloud.clients.metrics.CloudRequestFailure;
import org.apache.asterix.cloud.clients.metrics.CloudRequestListeners;
import org.apache.asterix.cloud.clients.metrics.CloudRequestOperation;

import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;

/**
 * Reports each S3 call to the {@link CloudRequestListeners}: its duration across all attempts, and why it failed.
 */
public final class S3RequestMetricsInterceptor implements ExecutionInterceptor {
    public static final S3RequestMetricsInterceptor INSTANCE = new S3RequestMetricsInterceptor();
    private static final ExecutionAttribute<Long> START_NANOS = new ExecutionAttribute<>("RequestMetricsStartNanos");

    private S3RequestMetricsInterceptor() {
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
        executionAttributes.putAttribute(START_NANOS, System.nanoTime());
    }

    @Override
    public void afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes) {
        report(executionAttributes, null);
    }

    @Override
    public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes executionAttributes) {
        report(executionAttributes, failure(context.exception()));
    }

    /**
     * Classifies a failed S3 call. The service error may arrive wrapped, as it does from the transfer manager.
     */
    static CloudRequestFailure failure(Throwable th) {
        for (Throwable cause = th; cause != null; cause = cause.getCause()) {
            if (cause instanceof SdkServiceException sse && sse.statusCode() > 0) {
                return sse.isThrottlingException() ? CloudRequestFailure.THROTTLED
                        : CloudRequestFailure.ofStatus(sse.statusCode());
            }
        }
        return CloudRequestFailure.ofTransportFailure(th);
    }

    private static void report(ExecutionAttributes executionAttributes, CloudRequestFailure failure) {
        Long start = executionAttributes.getAttribute(START_NANOS);
        if (start == null) {
            return;
        }
        CloudRequestListeners.requestCompleted(
                operation(executionAttributes.getAttribute(SdkExecutionAttribute.OPERATION_NAME)),
                System.nanoTime() - start, failure);
    }

    static CloudRequestOperation operation(String operationName) {
        if (operationName == null) {
            return CloudRequestOperation.OTHER;
        }
        return switch (operationName) {
            case "GetObject" -> CloudRequestOperation.GET;
            case "HeadObject" -> CloudRequestOperation.HEAD;
            case "ListObjects", "ListObjectsV2" -> CloudRequestOperation.LIST;
            case "PutObject" -> CloudRequestOperation.WRITE;
            case "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload", "AbortMultipartUpload" ->
                CloudRequestOperation.MULTIPART_UPLOAD;
            case "CopyObject" -> CloudRequestOperation.COPY;
            case "DeleteObject", "DeleteObjects" -> CloudRequestOperation.DELETE;
            default -> CloudRequestOperation.OTHER;
        };
    }
}
