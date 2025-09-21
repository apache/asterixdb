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
package org.apache.asterix.external.awsclient;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.aws.AwsUtils;
import org.apache.asterix.external.util.aws.AwsUtils.CloseableAwsClients;
import org.apache.asterix.external.util.aws.glue.GlueUtils;
import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.iceberg.aws.AwsClientFactory;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Custom AWS client factory that ensures assume-role resources (STS client + credentials provider)
 * are closed when the Glue catalog is closed. Achieved by returning a proxy GlueClient with a
 * close() method that performs the extra cleanup.
 */
public class EnsureCloseAWSClientFactory implements AwsClientFactory {
    private static final long serialVersionUID = 1L;

    private Map<String, String> properties;
    private CloseableAwsClients s3Clients;
    private CloseableAwsClients glueClients;
    private GlueClient proxyGlueClient; // cached proxy

    @Override
    public void initialize(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public S3Client s3() {
        ensureS3Clients();
        return (S3Client) s3Clients.getConsumingClient();
    }

    @Override
    public S3AsyncClient s3Async() {
        return null;
    }

    @Override
    public GlueClient glue() {
        ensureGlueClients();
        return (GlueClient) glueClients.getConsumingClient();
    }

    @Override
    public KmsClient kms() {
        return null;
    }

    @Override
    public software.amazon.awssdk.services.dynamodb.DynamoDbClient dynamo() {
        return null;
    }

    private void ensureS3Clients() {
        if (s3Clients == null) {
            try {
                s3Clients = S3Utils.buildClient(null, properties);
            } catch (CompilationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void ensureGlueClients() {
        if (glueClients == null) {
            try {
                glueClients = GlueUtils.buildClient(null, properties);
            } catch (CompilationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Invocation handler that intercepts close() to also close STS + credentials provider.
     */
    private class GlueClientInvocationHandler implements InvocationHandler {
        private final GlueClient delegate;
        private volatile boolean closed = false;

        GlueClientInvocationHandler(GlueClient delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if ("close".equals(name)) {
                if (!closed) {
                    closed = true;
                    try {
                        delegate.close();
                    } finally {
                        // Close both sets; no-op if not built or already closed
                        AwsUtils.closeClients(glueClients);
                        AwsUtils.closeClients(s3Clients);
                    }
                }
                return null;
            }
            return method.invoke(delegate, args);
        }
    }
}
