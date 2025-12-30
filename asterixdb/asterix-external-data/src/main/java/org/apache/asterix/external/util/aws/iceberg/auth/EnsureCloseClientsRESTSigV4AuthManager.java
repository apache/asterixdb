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
package org.apache.asterix.external.util.aws.iceberg.auth;

import static org.apache.asterix.external.util.aws.AwsUtils.buildCredentialsProvider;
import static org.apache.iceberg.rest.auth.OAuth2Util.AuthSession.empty;

import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.aws.AwsUtils;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;

/**
 * Copy implementation of {@link org.apache.iceberg.aws.RESTSigV4AuthManager} that:
 * - ensures the created AWS clients are closed properly.
 * - creates a credentials provider based on the provided configuration
 */
public class EnsureCloseClientsRESTSigV4AuthManager implements AuthManager {

    private final Aws4Signer signer = Aws4Signer.create();
    private final AuthManager delegate;
    private final AwsUtils.CloseableAwsClients awsClients = new AwsUtils.CloseableAwsClients();

    public EnsureCloseClientsRESTSigV4AuthManager(String ignored, AuthManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public EnsureCloseClientsRESTSigV4AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
        return catalogSession(initClient, properties);
    }

    @Override
    public EnsureCloseClientsRESTSigV4AuthSession catalogSession(RESTClient sharedClient,
            Map<String, String> properties) {
        AwsProperties awsProperties = new AwsProperties(properties);
        try {
            AuthSession authSession = delegate != null ? delegate.catalogSession(sharedClient, properties) : empty();
            AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(null, properties, awsClients);
            return new EnsureCloseClientsRESTSigV4AuthSession(signer, authSession, awsProperties, credentialsProvider);
        } catch (CompilationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        AwsUtils.closeClients(awsClients);
    }
}
