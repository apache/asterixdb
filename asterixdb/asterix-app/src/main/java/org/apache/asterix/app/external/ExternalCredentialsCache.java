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
    private final int refreshAwsAssumeRoleThresholdPercentage;

    public ExternalCredentialsCache(IApplicationContext appCtx) {
        this.awsAssumeRoleDuration = appCtx.getExternalProperties().getAwsAssumeRoleDuration();
        this.refreshAwsAssumeRoleThresholdPercentage =
                appCtx.getExternalProperties().getAwsRefreshAssumeRoleThresholdPercentage();
    }

    @Override
    public synchronized Object get(String key) {
        invalidateCache();
        if (cache.containsKey(key)) {
            return cache.get(key).getRight();
        }
        return null;
    }

    @Override
    public void delete(String key) {
        Object removed = cache.remove(key);
        if (removed != null) {
            LOGGER.info("Removed cached credentials for {} because it got deleted", key);
        }
    }

    @Override
    public synchronized void put(String key, String type, Map<String, String> credentials) {
        if (ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3.equalsIgnoreCase(type)) {
            updateAwsCache(key, credentials);
        }
    }

    private void updateAwsCache(String name, Map<String, String> credentials) {
        String accessKeyId = credentials.get(S3Constants.ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = credentials.get(S3Constants.SECRET_ACCESS_KEY_FIELD_NAME);
        String sessionToken = credentials.get(S3Constants.SESSION_TOKEN_FIELD_NAME);

        AwsSessionCredentials sessionCreds = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        cache.put(name, Pair.of(Span.start(awsAssumeRoleDuration, TimeUnit.SECONDS), sessionCreds));
        LOGGER.info("Received and cached new credentials for {}", name);
    }

    /**
     * Iterates the cache and removes the credentials that are considered expired
     */
    private void invalidateCache() {
        cache.entrySet().removeIf(entry -> {
            boolean shouldRemove = needsRefresh(entry.getValue().getLeft());
            if (shouldRemove) {
                LOGGER.info("Removing cached credentials for {} because it expired", entry.getKey());
            }
            return shouldRemove;
        });
    }

    /**
     * Refresh if the remaining time is less than the configured refresh percentage
     *
     * @param span expiration span
     * @return true if the remaining time is less than the configured refresh percentage, false otherwise
     */
    private boolean needsRefresh(Span span) {
        double remaining = (double) span.remaining(TimeUnit.SECONDS) / span.getSpan(TimeUnit.SECONDS);
        double passed = 1 - remaining;
        int passedPercentage = (int) (passed * 100);
        return passedPercentage > refreshAwsAssumeRoleThresholdPercentage;
    }
}
