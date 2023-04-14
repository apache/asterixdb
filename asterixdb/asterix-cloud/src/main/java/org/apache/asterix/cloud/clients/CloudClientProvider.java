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

import org.apache.asterix.cloud.clients.ICloudClientCredentialsProvider.CredentialsType;
import org.apache.asterix.cloud.clients.aws.s3.S3CloudClient;
import org.apache.asterix.cloud.clients.aws.s3.credentials.IS3Credentials;
import org.apache.asterix.cloud.clients.aws.s3.credentials.S3CredentialsProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class CloudClientProvider {

    public enum ClientType {
        NO_OP,
        S3,
        AZURE_BLOB,
        GOOGLE_CLOUD_STORAGE
    }

    private CloudClientProvider() {
        throw new AssertionError("do not instantiate");
    }

    public static ICloudClient getClient(ClientType clientType, CredentialsType credentialsType)
            throws HyracksDataException {
        switch (clientType) {
            case S3:
                IS3Credentials credentials = S3CredentialsProvider.INSTANCE.getCredentials(credentialsType);
                return new S3CloudClient(credentials);
            default:
                throw HyracksDataException.create(new IllegalArgumentException("Unknown cloud client type"));
        }
    }
}
