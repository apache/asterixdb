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
package org.apache.asterix.app.message;

import java.util.Map;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.external.util.ExternalDataConstants;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class UpdateAwsCredentialsCacheRequest implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final Map<String, String> configuration;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;

    public UpdateAwsCredentialsCacheRequest(Map<String, String> configuration, String accessKeyId,
            String secretAccessKey, String sessionToken) {
        this.configuration = configuration;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;

    }

    @Override
    public void handle(INcApplicationContext appCtx) {
        String name = configuration.get(ExternalDataConstants.KEY_ENTITY_ID);
        AwsSessionCredentials credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        appCtx.getExternalCredentialsCache().put(name, credentials);
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
