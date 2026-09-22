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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;

import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.IParallelDownloader;
import org.apache.hyracks.cloud.io.ICloudProperties;
import org.apache.hyracks.cloud.io.S3ChecksumBehavior;
import org.apache.hyracks.util.StorageUtil;
import org.junit.Test;

import software.amazon.awssdk.auth.credentials.AwsCredentials;

/**
 * Covers the configuration swap in {@link S3CloudClient#reloadConfiguration(ICloudProperties)}, the path a
 * live storage-settings change (endpoint, region, static credentials, path style, checksum behaviour,
 * certificates) takes into a running instance.
 * <p>
 * Reloading rebuilds the consuming client, but parallel downloaders are not long-lived: one is built per
 * download, from the {@link S3ClientConfig} the cloud client holds. While that field kept its construction
 * value, uploads and listings moved to the new endpoint and keys the moment the settings changed, and every
 * download went on riding the old ones until the process restarted -- with nothing in the logs to show the
 * split, since the reload line printed the config it had just failed to replace. Credential rotation is the
 * bite: downloads keep presenting keys that are about to be revoked.
 */
public class S3CloudClientReloadTest {

    private static final String BUCKET = "reload-test-bucket";

    private static final TestCloudProperties BEFORE = new TestCloudProperties("http://127.0.0.1:9101", "us-west-2",
            "old-access-key", "old-secret-key", false, S3ChecksumBehavior.WHEN_SUPPORTED, "SYNC");
    private static final TestCloudProperties AFTER = new TestCloudProperties("http://127.0.0.1:9202", "eu-central-1",
            "new-access-key", "new-secret-key", true, S3ChecksumBehavior.WHEN_REQUIRED, "SYNC");

    /**
     * The regression: a downloader created after the reload was handed the pre-reload endpoint, region,
     * credentials, path style and checksum behaviour.
     */
    @Test
    public void downloaderCreatedAfterReloadUsesTheNewConfiguration() throws Exception {
        S3CloudClient client = newClient(BEFORE);
        try {
            client.reloadConfiguration(AFTER);
            IParallelDownloader downloader = client.createParallelDownloader(BUCKET, null);
            try {
                S3ClientConfig used = configOf(downloader);
                assertEquals("endpoint", AFTER.endpoint, used.getEndpoint());
                assertEquals("region", AFTER.region, used.getRegion());
                assertEquals("checksum behavior", AFTER.checksumBehavior, used.getChecksumBehavior());
                assertTrue("path style", used.isForcePathStyle());
                assertCredentials(AFTER.accessKeyId, AFTER.secretAccessKey, used);
            } finally {
                downloader.close();
            }
        } finally {
            client.close();
        }
    }

    /**
     * A downloader created before the reload keeps the configuration it was built with -- it owns its own SDK
     * client, so there is nothing to swap in it. Downloads are short-lived, so the next one picks the new
     * settings up; this pins the boundary rather than asserting a reload reaches back into live downloaders.
     */
    @Test
    public void downloaderCreatedBeforeReloadKeepsItsOwnConfiguration() throws Exception {
        S3CloudClient client = newClient(BEFORE);
        try {
            IParallelDownloader downloader = client.createParallelDownloader(BUCKET, null);
            try {
                client.reloadConfiguration(AFTER);
                S3ClientConfig used = configOf(downloader);
                assertEquals("endpoint", BEFORE.endpoint, used.getEndpoint());
                assertFalse("path style", used.isForcePathStyle());
                assertCredentials(BEFORE.accessKeyId, BEFORE.secretAccessKey, used);
            } finally {
                downloader.close();
            }
        } finally {
            client.close();
        }
    }

    /**
     * The client type is read from the same field, so the reload has to be able to change which downloader
     * implementation is built -- it is one of the settings an operator can change at runtime.
     */
    @Test
    public void reloadChangesTheDownloaderImplementation() throws Exception {
        S3CloudClient client = newClient(BEFORE);
        try {
            IParallelDownloader before = client.createParallelDownloader(BUCKET, null);
            before.close();
            assertTrue("expected a sync downloader before reload", before instanceof S3SyncDownloader);

            client.reloadConfiguration(AFTER.withDownloaderClientType("ASYNC"));
            IParallelDownloader after = client.createParallelDownloader(BUCKET, null);
            try {
                assertTrue("expected an async downloader after reload, was " + after.getClass().getSimpleName(),
                        after instanceof S3ParallelDownloader);
            } finally {
                after.close();
            }
        } finally {
            client.close();
        }
    }

    private static S3CloudClient newClient(ICloudProperties properties) {
        return new S3CloudClient(S3ClientConfig.of(properties), ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    private static void assertCredentials(String expectedAccessKeyId, String expectedSecretAccessKey,
            S3ClientConfig config) {
        AwsCredentials credentials = config.createCredentialsProvider().resolveCredentials();
        assertEquals("access key id", expectedAccessKeyId, credentials.accessKeyId());
        assertEquals("secret access key", expectedSecretAccessKey, credentials.secretAccessKey());
    }

    /** The config a downloader was handed; it is what every request that downloader makes is built from. */
    private static S3ClientConfig configOf(IParallelDownloader downloader) throws Exception {
        Field field = downloader.getClass().getDeclaredField("config");
        field.setAccessible(true);
        return (S3ClientConfig) field.get(downloader);
    }

    /**
     * The settings a reload carries, with everything else at a value that keeps the SDK clients cheap to
     * build: no profiler, no rate limiting, no connection pool overrides.
     */
    private static final class TestCloudProperties implements ICloudProperties {

        private final String endpoint;
        private final String region;
        private final String accessKeyId;
        private final String secretAccessKey;
        private final boolean forcePathStyle;
        private final S3ChecksumBehavior checksumBehavior;
        private final String downloaderClientType;

        private TestCloudProperties(String endpoint, String region, String accessKeyId, String secretAccessKey,
                boolean forcePathStyle, S3ChecksumBehavior checksumBehavior, String downloaderClientType) {
            this.endpoint = endpoint;
            this.region = region;
            this.accessKeyId = accessKeyId;
            this.secretAccessKey = secretAccessKey;
            this.forcePathStyle = forcePathStyle;
            this.checksumBehavior = checksumBehavior;
            this.downloaderClientType = downloaderClientType;
        }

        private TestCloudProperties withDownloaderClientType(String newDownloaderClientType) {
            return new TestCloudProperties(endpoint, region, accessKeyId, secretAccessKey, forcePathStyle,
                    checksumBehavior, newDownloaderClientType);
        }

        @Override
        public String getStorageEndpoint() {
            return endpoint;
        }

        @Override
        public String getStorageRegion() {
            return region;
        }

        @Override
        public String getS3AccessKeyId() {
            return accessKeyId;
        }

        @Override
        public String getS3SecretAccessKey() {
            return secretAccessKey;
        }

        @Override
        public boolean isStorageForcePathStyle() {
            return forcePathStyle;
        }

        @Override
        public S3ChecksumBehavior getS3ChecksumBehavior() {
            return checksumBehavior;
        }

        @Override
        public String getS3ParallelDownloaderClientType() {
            return downloaderClientType;
        }

        @Override
        public String getStorageScheme() {
            return "s3";
        }

        @Override
        public String getStorageBucket() {
            return BUCKET;
        }

        @Override
        public String getStoragePrefix() {
            return "";
        }

        @Override
        public boolean isStorageAnonymousAuth() {
            return false;
        }

        @Override
        public Collection<String> getStorageCertificates() {
            return Collections.emptyList();
        }

        @Override
        public double getStorageAllocationPercentage() {
            return 0;
        }

        @Override
        public double getStorageSweepThresholdPercentage() {
            return 0;
        }

        @Override
        public int getStorageDiskMonitorInterval() {
            return 0;
        }

        @Override
        public long getStorageIndexInactiveDurationThreshold() {
            return 0;
        }

        @Override
        public boolean isStorageDebugModeEnabled() {
            return false;
        }

        @Override
        public long getStorageDebugSweepThresholdSize() {
            return 0;
        }

        @Override
        public long getProfilerLogInterval() {
            return 0;
        }

        @Override
        public long getTokenAcquireTimeout() {
            return 1;
        }

        @Override
        public int getWriteMaxRequestsPerSecond() {
            return 0;
        }

        @Override
        public int getReadMaxRequestsPerSecond() {
            return 0;
        }

        @Override
        public int getWriteBufferSize() {
            return StorageUtil.getIntSizeInBytes(5, StorageUtil.StorageUnit.MEGABYTE);
        }

        @Override
        public int getEvictionPlanReevaluationThreshold() {
            return 0;
        }

        @Override
        public int getRequestsMaxHttpConnections() {
            return 0;
        }

        @Override
        public int getRequestsMaxPendingHttpConnections() {
            return 0;
        }

        @Override
        public int getRequestsHttpConnectionAcquireTimeout() {
            return 0;
        }

        @Override
        public int getRequestsHttpConnectionMaxIdleSeconds() {
            return 0;
        }

        @Override
        public int getRequestsHttpConnectionMaxLifetimeSeconds() {
            return 0;
        }

        @Override
        public boolean isStorageDisableSSLVerify() {
            return false;
        }

        @Override
        public int getS3ReadTimeoutInSeconds() {
            return -1;
        }

        @Override
        public boolean useRoundRobinDnsResolver() {
            return false;
        }

        @Override
        public String getAzureClientId() {
            return null;
        }
    }
}
