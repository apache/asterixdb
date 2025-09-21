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
package org.apache.asterix.api.common;

import static org.apache.asterix.api.common.LocalCloudUtil.MOCK_SERVER_REGION;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

// This runs Adobe S3Mock in a docker container, with the bucket storage directory under target/s3mock.
// Search for line "Successfully created {} as root folder" in the info log file
// A randomized port is chosen each time for the S3 endpoint, but it is always on localhost.
// You can inspect the real port mapping (e.g. 45799 -> 9090) via 'docker ps'
// Then to connect, normal S3 utilities work, such as aws s3 ls --endpoint http://localhost:45799
public class LocalCloudUtilAdobeMock {

    private static final Logger LOGGER = LogManager.getLogger();

    public static final String CLOUD_STORAGE_BUCKET = "cloud-storage-container";
    public static final String S3MOCK_VERSION_TAG = "4.7.0";
    public static final String PLAYGROUND_BUCKET = "playground";
    public static final String CLOUD_URL_KEY = "cloudUrl";
    private static S3MockContainer s3Mock;

    private LocalCloudUtilAdobeMock() {
        throw new AssertionError("Do not instantiate");
    }

    public static void main(String[] args) throws IOException {
        String cleanStartString = System.getProperty("cleanup.start", "true");
        boolean cleanStart = Boolean.parseBoolean(cleanStartString);
        // Change to 'true' if you want to delete "s3mock" folder on start
        startS3CloudEnvironment(cleanStart);
    }

    public static S3MockContainer startS3CloudEnvironment(boolean cleanStart) throws IOException {
        return startS3CloudEnvironment(cleanStart, false);
    }

    public static S3MockContainer startS3CloudEnvironment(boolean cleanStart, boolean createPlaygroundContainer)
            throws IOException {
        // Starting S3 mock server to be used instead of real S3 server
        LOGGER.info("Starting S3 mock server");
        s3Mock = new S3MockContainer(S3MOCK_VERSION_TAG).withRetainFilesOnExit(!cleanStart);
        if (!cleanStart) {
            Path s3MockDataDir = Path.of("target", "s3mock");
            boolean existingData = s3MockDataDir.toFile().exists();
            if (!existingData) {
                Files.createDirectory(s3MockDataDir);
            }
            //be VERY careful with this path. the last component will be entirely deleted
            //if RetainFilesOnExist is false
            s3Mock.withVolumeAsRoot(s3MockDataDir.toAbsolutePath());
        }
        s3Mock.start();
        LOGGER.info("S3 mock server started successfully");

        S3ClientBuilder builder = S3Client.builder();
        URI endpoint = URI.create(s3Mock.getHttpEndpoint()); // endpoint pointing to S3 mock server
        builder.region(Region.of(MOCK_SERVER_REGION)).credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(endpoint);
        S3Client client =
                builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build();
        createIfNotExists(CLOUD_STORAGE_BUCKET, client);
        if (createPlaygroundContainer) {
            createIfNotExists(PLAYGROUND_BUCKET, client);
        }
        client.close();
        return s3Mock;
    }

    public static void createIfNotExists(String bucketName, S3Client client) {
        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
        } catch (NoSuchBucketException nsbe) {
            //i don't want to use exceptions as control flow, yet here we are
            client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            LOGGER.info("Created bucket {}", bucketName);
        }
    }

    public static void fillConfigTemplate(String cloudUrl, String templatePath, String configPath)
            throws IOException, TemplateException {
        try (FileReader tmplt = new FileReader(templatePath); Writer result = new FileWriter(configPath)) {
            Template cfg = new Template("cc.conf", tmplt, new Configuration(Configuration.VERSION_2_3_31));
            cfg.process(Map.of(CLOUD_URL_KEY, cloudUrl), result);
        }

    }

    public static void shutdownSilently() {
        if (s3Mock != null) {
            try {
                LOGGER.info("test cleanup, stopping S3 mock server");
                s3Mock.stop();
                s3Mock.close();
                LOGGER.info("test cleanup, stopped S3 mock server");
            } catch (Exception ex) {
                // do nothing
            }
            s3Mock = null;
        }
    }
}
