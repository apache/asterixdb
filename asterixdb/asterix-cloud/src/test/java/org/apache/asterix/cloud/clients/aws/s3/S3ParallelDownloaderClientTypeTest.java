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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.cloud.clients.aws.s3.S3ClientConfig.S3ParallelDownloaderClientType;
import org.apache.asterix.cloud.clients.profiler.NoOpRequestProfilerLimiter;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.hyracks.util.StorageUtil;
import org.junit.Test;

/**
 * The parallel downloader client type is an explicit choice: its default does not depend on whether an endpoint
 * is configured, and the CRT client refuses a configuration it cannot honour.
 */
public class S3ParallelDownloaderClientTypeTest {

    private static final String ENDPOINT = "https://127.0.0.1:9";
    private static final int WRITE_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(8, StorageUtil.StorageUnit.MEGABYTE);
    // the guard fires before any certificate is parsed, so the content only has to be non-empty
    private static final List<String> CERTIFICATES =
            Collections.singletonList("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----");

    /**
     * The default used to be a function of the endpoint ({@code crt} without one, {@code async} with one), a
     * leftover from when a configured endpoint could only mean the tests' S3 mock. It is now a plain constant.
     */
    @Test
    public void defaultClientTypeIsAConstantCrt() {
        Object defaultValue = CloudProperties.Option.CLOUD_STORAGE_S3_PARALLEL_DOWNLOADER_CLIENT_TYPE.defaultValue();
        assertTrue("default should be a literal, was " + defaultValue.getClass().getName(),
                defaultValue instanceof String);
        assertEquals("crt", defaultValue);
    }

    @Test
    public void crtWithCertificatesIsRejected() {
        S3ClientConfig config = config(S3ParallelDownloaderClientType.CRT, CERTIFICATES);
        try {
            new S3ParallelDownloader("bucket", null, config, NoOpRequestProfilerLimiter.INSTANCE).close();
            fail("expected the crt client to refuse custom certificates");
        } catch (IllegalArgumentException e) {
            assertEquals(S3ParallelDownloader.CRT_CERTIFICATES_MESSAGE, e.getMessage());
        }
    }

    /**
     * The endpoint alone is no reason to avoid the CRT client: the builder takes an endpoint override, and a
     * downloader can be built against one.
     */
    @Test
    public void crtWithEndpointAndNoCertificatesBuilds() {
        S3ClientConfig config = config(S3ParallelDownloaderClientType.CRT, Collections.emptyList());
        new S3ParallelDownloader("bucket", null, config, NoOpRequestProfilerLimiter.INSTANCE).close();
    }

    private static S3ClientConfig config(S3ParallelDownloaderClientType clientType, Collection<String> certificates) {
        return new S3ClientConfig("us-west-2", ENDPOINT, "", true, certificates, 0, WRITE_BUFFER_SIZE, clientType,
                false);
    }
}
