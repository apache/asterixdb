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
package org.apache.asterix.cloud.clients;

import org.apache.asterix.cloud.clients.aws.s3.S3ClientConfig;
import org.apache.asterix.cloud.clients.aws.s3.S3CloudClient;
import org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageClientConfig;
import org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageCloudClient;
import org.apache.asterix.cloud.clients.google.gcs.GCSClientConfig;
import org.apache.asterix.cloud.clients.google.gcs.GCSCloudClient;
import org.apache.asterix.common.config.CloudProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.cloud.util.CloudRetryableRequestUtil;

public class CloudClientProvider {
    private static final boolean UNSTABLE = isUnstable();
    public static final String S3 = "s3";
    public static final String GCS = "gs";
    public static final String AZ_BLOB = "azblob";

    private CloudClientProvider() {
        throw new AssertionError("do not instantiate");
    }

    public static ICloudClient getClient(CloudProperties cloudProperties, ICloudGuardian guardian)
            throws HyracksDataException {
        String storageScheme = cloudProperties.getStorageScheme();
        ICloudClient cloudClient;
        if (S3.equalsIgnoreCase(storageScheme)) {
            S3ClientConfig config = S3ClientConfig.of(cloudProperties);
            cloudClient = new S3CloudClient(config, guardian);
        } else if (GCS.equalsIgnoreCase(storageScheme)) {
            GCSClientConfig config = GCSClientConfig.of(cloudProperties);
            cloudClient = new GCSCloudClient(config, guardian);
        } else if (AZ_BLOB.equalsIgnoreCase(storageScheme)) {
            AzBlobStorageClientConfig config = AzBlobStorageClientConfig.of(cloudProperties);
            cloudClient = new AzBlobStorageCloudClient(config, guardian);
        } else {
            throw new IllegalStateException("unsupported cloud storage scheme: " + storageScheme);
        }

        return UNSTABLE ? new UnstableCloudClient(cloudClient) : cloudClient;
    }

    private static boolean isUnstable() {
        return Boolean.getBoolean(CloudRetryableRequestUtil.CLOUD_UNSTABLE_MODE);
    }
}
