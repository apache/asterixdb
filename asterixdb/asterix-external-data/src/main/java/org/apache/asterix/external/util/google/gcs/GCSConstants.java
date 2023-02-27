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
package org.apache.asterix.external.util.google.gcs;

public class GCSConstants {
    private GCSConstants() {
        throw new AssertionError("do not instantiate");
    }

    public static final String APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME = "applicationDefaultCredentials";
    public static final String JSON_CREDENTIALS_FIELD_NAME = "jsonCredentials";
    public static final String ENDPOINT_FIELD_NAME = "endpoint";

    /*
     * Hadoop internal configuration
     */
    public static final String HADOOP_GCS_PROTOCOL = "gs";

    // hadoop credentials
    public static final String HADOOP_AUTH_TYPE = "fs.gs.auth.type";
    public static final String HADOOP_AUTH_UNAUTHENTICATED = "UNAUTHENTICATED";
    public static final String HADOOP_AUTH_SERVICE_ACCOUNT_JSON_KEY_FILE = "SERVICE_ACCOUNT_JSON_KEYFILE";
    public static final String HADOOP_AUTH_SERVICE_ACCOUNT_JSON_KEY_FILE_PATH =
            "google.cloud.auth.service.account.json.keyfile";

    // gs hadoop parameters
    public static final String HADOOP_SUPPORT_COMPRESSED = "fs.gs.inputstream.support.gzip.encoding.enable";
    public static final String HADOOP_ENDPOINT = "fs.gs.storage.root.url";
    public static final String HADOOP_MAX_REQUESTS_PER_BATCH = "fs.gs.max.requests.per.batch";
    public static final String HADOOP_BATCH_THREADS = "fs.gs.batch.threads";
}
