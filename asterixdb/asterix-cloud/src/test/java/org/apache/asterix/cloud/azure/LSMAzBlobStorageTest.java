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

package org.apache.asterix.cloud.azure;

import org.apache.asterix.cloud.AbstractLSMTest;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageClientConfig;
import org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageCloudClient;
import org.apache.hyracks.util.StorageUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;

public class LSMAzBlobStorageTest extends AbstractLSMTest {
    private static BlobContainerClient client;

    private static BlobServiceClient blobServiceClient;
    private static final int MOCK_SERVER_PORT = 15055;
    private static final String MOCK_SERVER_HOSTNAME = "http://127.0.0.1:" + MOCK_SERVER_PORT;
    private static final String MOCK_SERVER_REGION = "us-west-2";

    @BeforeClass
    public static void setup() throws Exception {
        LOGGER.info("LSMAzBlobStorageTest setup");

        String endpointString = "http://127.0.0.1:15055/devstoreaccount1/" + PLAYGROUND_CONTAINER;
        final String accKey =
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        final String accName = "devstoreaccount1";

        blobServiceClient = new BlobServiceClientBuilder().endpoint(endpointString)
                .credential(new StorageSharedKeyCredential(accName, accKey)).buildClient();

        // Start the test clean by deleting any residual data from previous tests
        blobServiceClient.deleteBlobContainerIfExists(PLAYGROUND_CONTAINER);
        client = blobServiceClient.createBlobContainerIfNotExists(PLAYGROUND_CONTAINER);

        LOGGER.info("Az Blob Client created successfully");
        int writeBufferSize = StorageUtil.getIntSizeInBytes(5, StorageUtil.StorageUnit.MEGABYTE);
        AzBlobStorageClientConfig config = new AzBlobStorageClientConfig(MOCK_SERVER_REGION, MOCK_SERVER_HOSTNAME, "",
                true, 0, PLAYGROUND_CONTAINER, 1, 0, 0, writeBufferSize);
        CLOUD_CLIENT = new AzBlobStorageCloudClient(config, ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    private static void cleanup() {
        try {
            PagedIterable<BlobItem> blobItems = client.listBlobs(new ListBlobsOptions().setPrefix(""), null);
            // Delete all the contents of the container
            for (BlobItem blobItem : blobItems) {
                BlobClient blobClient = client.getBlobClient(blobItem.getName());
                blobClient.delete();
            }
            // Delete the container
            blobServiceClient.deleteBlobContainer(PLAYGROUND_CONTAINER);
        } catch (Exception ex) {
            // ignore
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LOGGER.info("Shutdown Azurite");
        // Azure clients do not need explicit closure.
        cleanup();
    }
}
