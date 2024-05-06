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

import static org.apache.asterix.external.util.google.gcs.GCSConstants.ENDPOINT_FIELD_NAME;

import java.io.IOException;
import java.util.Map;

import org.apache.asterix.common.config.CloudProperties;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.StorageUtil;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;

public class GCSClientConfig {
    public static final int WRITE_BUFFER_SIZE = StorageUtil.getIntSizeInBytes(1, StorageUtil.StorageUnit.MEGABYTE);
    // The maximum number of files that can be deleted (GCS restriction): https://cloud.google.com/storage/quotas#json-requests
    static final int DELETE_BATCH_SIZE = 100;
    private final String region;
    private final String endpoint;
    private final String prefix;
    private final boolean anonymousAuth;
    private final long profilerLogInterval;

    public GCSClientConfig(String region, String endpoint, String prefix, boolean anonymousAuth,
            long profilerLogInterval) {
        this.region = region;
        this.endpoint = endpoint;
        this.prefix = prefix;
        this.anonymousAuth = anonymousAuth;
        this.profilerLogInterval = profilerLogInterval;
    }

    public static GCSClientConfig of(CloudProperties cloudProperties) {
        return new GCSClientConfig(cloudProperties.getStorageRegion(), cloudProperties.getStorageEndpoint(),
                cloudProperties.getStoragePrefix(), cloudProperties.isStorageAnonymousAuth(),
                cloudProperties.getProfilerLogInterval());
    }

    public static GCSClientConfig of(Map<String, String> configuration) {
        String endPoint = configuration.getOrDefault(ENDPOINT_FIELD_NAME, "");
        long profilerLogInterval = 0;

        String region = "";
        String prefix = "";
        boolean anonymousAuth = false;

        return new GCSClientConfig(region, endPoint, prefix, anonymousAuth, profilerLogInterval);
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getPrefix() {
        return prefix;
    }

    public long getProfilerLogInterval() {
        return profilerLogInterval;
    }

    public boolean isAnonymousAuth() {
        return anonymousAuth;
    }

    public OAuth2Credentials createCredentialsProvider() throws HyracksDataException {
        try {
            return anonymousAuth ? NoCredentials.getInstance() : GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
