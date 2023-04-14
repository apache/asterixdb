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
package org.apache.asterix.cloud.clients.aws.s3.credentials;

public class S3MockCredentials implements IS3Credentials {

    private static final String ACCESS_KEY_ID = "dummyAccessKeyId";
    private static final String SECRET_ACCESS_KEY = "dummySecretAccessKey";
    private static final String REGION = "us-west-2";
    private static final String ENDPOINT = "http://127.0.0.1:8001";

    public static final S3MockCredentials INSTANCE = new S3MockCredentials();

    private S3MockCredentials() {
    }

    @Override
    public String getAccessKeyId() {
        return ACCESS_KEY_ID;
    }

    @Override
    public String getSecretAccessKey() {
        return SECRET_ACCESS_KEY;
    }

    @Override
    public String getRegion() {
        return REGION;
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }
}
