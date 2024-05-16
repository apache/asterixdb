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
package org.apache.asterix.cloud.gcs;

import org.apache.asterix.cloud.AbstractLSMTest;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.google.gcs.GCSClientConfig;
import org.apache.asterix.cloud.clients.google.gcs.GCSCloudClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageOptions;

public class LSMGCSTest extends AbstractLSMTest {
    private static Storage client;
    private static final int MOCK_SERVER_PORT = 4443;
    private static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;
    private static final String MOCK_SERVER_REGION = "us-west2"; // does not matter the value
    private static final String MOCK_SERVER_PROJECT_ID = "asterixdb-gcs-test-project-id";

    @BeforeClass
    public static void setup() throws Exception {
        client = StorageOptions.newBuilder().setHost(MOCK_SERVER_HOSTNAME).setCredentials(NoCredentials.getInstance())
                .setProjectId(MOCK_SERVER_PROJECT_ID).build().getService();

        cleanup();
        client.create(BucketInfo.newBuilder(PLAYGROUND_CONTAINER).setStorageClass(StorageClass.STANDARD)
                .setLocation(MOCK_SERVER_REGION).build());
        LOGGER.info("Client created successfully");
        GCSClientConfig config = new GCSClientConfig(MOCK_SERVER_REGION, MOCK_SERVER_HOSTNAME, "", true, 0);
        CLOUD_CLIENT = new GCSCloudClient(config, ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    private static void cleanup() {
        try {
            Iterable<Blob> blobs = client.list(PLAYGROUND_CONTAINER).iterateAll();
            blobs.forEach(Blob::delete);
            client.delete(PLAYGROUND_CONTAINER);
        } catch (Exception ex) {
            // ignore
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // Shutting down GCS mock server
        LOGGER.info("Shutting down GCS mock client");
        if (client != null) {
            client.close();
        }
        LOGGER.info("GCS mock client shut down successfully");
    }
}
