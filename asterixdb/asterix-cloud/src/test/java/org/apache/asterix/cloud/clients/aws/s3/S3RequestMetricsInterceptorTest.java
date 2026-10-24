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

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletionException;

import org.apache.asterix.cloud.clients.metrics.CloudRequestFailure;
import org.apache.asterix.cloud.clients.metrics.CloudRequestOperation;
import org.junit.Assert;
import org.junit.Test;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3RequestMetricsInterceptorTest {

    @Test
    public void mapsOperationNames() {
        Assert.assertEquals(CloudRequestOperation.GET, S3RequestMetricsInterceptor.operation("GetObject"));
        Assert.assertEquals(CloudRequestOperation.LIST, S3RequestMetricsInterceptor.operation("ListObjectsV2"));
        Assert.assertEquals(CloudRequestOperation.MULTIPART_UPLOAD,
                S3RequestMetricsInterceptor.operation("UploadPart"));
        Assert.assertEquals(CloudRequestOperation.DELETE, S3RequestMetricsInterceptor.operation("DeleteObjects"));
        Assert.assertEquals(CloudRequestOperation.OTHER, S3RequestMetricsInterceptor.operation("GetBucketPolicy"));
        Assert.assertEquals(CloudRequestOperation.OTHER, S3RequestMetricsInterceptor.operation(null));
    }

    @Test
    public void classifiesFailures() {
        Assert.assertEquals(CloudRequestFailure.NOT_FOUND, CloudRequestFailure.ofStatus(404));
        Assert.assertEquals(CloudRequestFailure.ACCESS_DENIED, CloudRequestFailure.ofStatus(403));
        Assert.assertEquals(CloudRequestFailure.THROTTLED, CloudRequestFailure.ofStatus(503));
        Assert.assertEquals(CloudRequestFailure.SERVER_ERROR, CloudRequestFailure.ofStatus(500));
        Assert.assertEquals(CloudRequestFailure.CLIENT_ERROR, CloudRequestFailure.ofStatus(400));
        Assert.assertEquals(CloudRequestFailure.TIMEOUT,
                CloudRequestFailure.ofTransportFailure(new RuntimeException(new SocketTimeoutException())));
        Assert.assertEquals(CloudRequestFailure.CONNECTION,
                CloudRequestFailure.ofTransportFailure(new RuntimeException(new IOException("reset"))));
        Assert.assertEquals(CloudRequestFailure.OTHER, CloudRequestFailure.ofTransportFailure(new RuntimeException()));
    }

    @Test
    public void findsWrappedServiceErrors() {
        // the transfer manager reports a failed download as an SdkClientException wrapping the service error
        Throwable wrapped = new CompletionException(SdkClientException.builder()
                .cause(S3Exception.builder().statusCode(403).message("denied").build()).build());
        Assert.assertEquals(CloudRequestFailure.ACCESS_DENIED, S3RequestMetricsInterceptor.failure(wrapped));
        Assert.assertEquals(CloudRequestFailure.CONNECTION,
                S3RequestMetricsInterceptor.failure(new CompletionException(new IOException("reset"))));
    }
}
