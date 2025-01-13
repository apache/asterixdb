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
package org.apache.asterix.app.external;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.external.IExternalCredentialsCache;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.util.Span;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class ExternalCredentialsCache implements IExternalCredentialsCache {

    private static final Logger LOGGER = LogManager.getLogger();
    private final ConcurrentMap<String, Pair<Span, Object>> cache = new ConcurrentHashMap<>();
    private final int awsAssumeRoleDuration;
    private final double refreshAwsAssumeRoleThreshold;

    public ExternalCredentialsCache(IApplicationContext appCtx) {
        this.awsAssumeRoleDuration = appCtx.getExternalProperties().getAwsAssumeRoleDuration();
        this.refreshAwsAssumeRoleThreshold = appCtx.getExternalProperties().getAwsRefreshAssumeRoleThreshold();
    }

    @Override
    public synchronized Object getCredentials(Map<String, String> configuration) {
        String name = getName(configuration);
        if (cache.containsKey(name) && !needsRefresh(cache.get(name).getLeft())) {
            return cache.get(name).getRight();
        }
        return null;
    }

    @Override
    public synchronized void updateCache(Map<String, String> configuration, Map<String, String> credentials) {
        String type = configuration.get(ExternalDataConstants.KEY_READER);
        if (ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3.equalsIgnoreCase(type)) {
            updateAwsCache(configuration, credentials);
        }
    }

    @Override
    public void deleteCredentials(String name) {
        cache.remove(name);
    }

    @Override
    public String getName(Map<String, String> configuration) {
        String database = configuration.get(ExternalDataConstants.KEY_DATASET_DATABASE);
        if (database == null) {
            database = MetadataConstants.DEFAULT_DATABASE;
        }
        String dataverse = configuration.get(ExternalDataConstants.KEY_DATASET_DATAVERSE);
        String dataset = configuration.get(ExternalDataConstants.KEY_DATASET);
        return String.join(".", database, dataverse, dataset);
    }

    private void updateAwsCache(Map<String, String> configuration, Map<String, String> credentials) {
        String accessKeyId = credentials.get(S3Constants.ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = credentials.get(S3Constants.SECRET_ACCESS_KEY_FIELD_NAME);
        String sessionToken = credentials.get(S3Constants.SESSION_TOKEN_FIELD_NAME);
        doUpdateAwsCache(configuration, AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
    }

    private void doUpdateAwsCache(Map<String, String> configuration, AwsSessionCredentials credentials) {
        String name = getName(configuration);
        cache.put(name, Pair.of(Span.start(awsAssumeRoleDuration, TimeUnit.SECONDS), credentials));
        LOGGER.info("Received and cached new credentials for {}", name);
    }

    /**
     * Refresh if the remaining time is less than the configured refresh percentage
     *
     * @param span expiration span
     * @return true if the remaining time is less than the configured refresh percentage, false otherwise
     */
    private boolean needsRefresh(Span span) {
        return (double) span.remaining(TimeUnit.SECONDS)
                / span.getSpan(TimeUnit.SECONDS) < refreshAwsAssumeRoleThreshold;
    }
}
