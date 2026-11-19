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
package org.apache.asterix.cloud.clients.google.gcs;

import static org.apache.asterix.cloud.clients.google.gcs.GCSRequestMetricsHttpClient.operation;

import org.apache.asterix.cloud.clients.metrics.CloudRequestOperation;
import org.junit.Assert;
import org.junit.Test;

public class GCSRequestMetricsHttpClientTest {
    private static final String API = "https://storage.googleapis.com/storage/v1/b/bkt";
    private static final String UPLOAD = "https://storage.googleapis.com/upload/storage/v1/b/bkt";

    @Test
    public void mapsJsonApiRequests() {
        Assert.assertEquals(CloudRequestOperation.GET, operation("GET", API + "/o/dir%2Fobj?alt=media"));
        Assert.assertEquals(CloudRequestOperation.HEAD, operation("GET", API + "/o/dir%2Fobj?projection=full"));
        Assert.assertEquals(CloudRequestOperation.LIST, operation("GET", API + "/o?prefix=dir%2F"));
        Assert.assertEquals(CloudRequestOperation.WRITE, operation("POST", UPLOAD + "/o?uploadType=multipart"));
        Assert.assertEquals(CloudRequestOperation.MULTIPART_UPLOAD,
                operation("POST", UPLOAD + "/o?uploadType=resumable&name=obj"));
        Assert.assertEquals(CloudRequestOperation.MULTIPART_UPLOAD,
                operation("PUT", UPLOAD + "/o?uploadType=resumable&upload_id=abc"));
        Assert.assertEquals(CloudRequestOperation.COPY, operation("POST", API + "/o/a/rewriteTo/b/bkt/o/b"));
        Assert.assertEquals(CloudRequestOperation.DELETE, operation("DELETE", API + "/o/obj"));
        Assert.assertEquals(CloudRequestOperation.DELETE,
                operation("POST", "https://storage.googleapis.com/batch/storage/v1"));
        Assert.assertEquals(CloudRequestOperation.OTHER, operation("GET", API));
    }

    @Test
    public void readsTheBucketAndObjectSegmentsByPosition() {
        // a bucket or an object named "o" must not be mistaken for the object collection
        Assert.assertEquals(CloudRequestOperation.LIST,
                operation("GET", "https://storage.googleapis.com/storage/v1/b/o/o?prefix=x"));
        Assert.assertEquals(CloudRequestOperation.GET,
                operation("GET", "https://storage.googleapis.com/storage/v1/b/o/o/o?alt=media"));
    }
}
