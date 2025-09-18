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

import static org.apache.asterix.api.common.AsterixHyracksIntegrationUtil.LoggerHolder.LOGGER;
import static org.apache.asterix.api.common.LocalCloudUtilAdobeMock.fillConfigTemplate;
import static org.apache.asterix.test.cloud_storage.CloudStorageTest.MOCK_SERVER_HOSTNAME_FRAGMENT;
import static org.apache.hyracks.util.file.FileUtil.joinPath;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;

public class CloudStorageIntegrationUtil extends AsterixHyracksIntegrationUtil {

    public static final String RESOURCES_PATH = joinPath(getProjectPath().toString(), "src", "test", "resources");
    public static final String CONFIG_FILE = joinPath(RESOURCES_PATH, "cc-cloud-storage-main.conf");
    public static final String CONFIG_FILE_TEMPLATE = joinPath(RESOURCES_PATH, "cc-cloud-storage-main.ftl");

    public static void main(String[] args) throws Exception {
        boolean cleanStart = Boolean.getBoolean("cleanup.start");
        LocalCloudUtilAdobeMock.startS3CloudEnvironment(cleanStart);
        final AsterixHyracksIntegrationUtil integrationUtil = new AsterixHyracksIntegrationUtil();
        try (S3MockContainer s3Mock = LocalCloudUtilAdobeMock.startS3CloudEnvironment(cleanStart)) {
            fillConfigTemplate(MOCK_SERVER_HOSTNAME_FRAGMENT + s3Mock.getHttpServerPort(), CONFIG_FILE_TEMPLATE,
                    CONFIG_FILE);
            integrationUtil.run(cleanStart, Boolean.getBoolean("cleanup.shutdown"),
                    System.getProperty("external.lib", ""), CONFIG_FILE);
        } catch (Exception e) {
            LOGGER.fatal("Unexpected exception", e);
            System.exit(1);
        }
    }
}
