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

import java.io.Serializable;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.aws.AwsUtils;
import org.apache.asterix.external.util.aws.AwsUtils.CloseableAwsClients;
import org.apache.asterix.external.util.aws.EnsureCloseClientsFactoryRegistry;
import org.apache.asterix.external.util.aws.iceberg.glue.GlueUtils;
import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.iceberg.aws.AwsClientFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

public class EnsureCloseAWSClientFactory implements AwsClientFactory, AutoCloseable, Serializable {
    private static final long serialVersionUID = 1L;

    private transient Map<String, String> properties;
    private transient String instanceId;

    private transient volatile CloseableAwsClients s3Clients;
    private transient volatile CloseableAwsClients glueClients;

    private transient volatile boolean closed;
    private transient volatile boolean registered;

    @Override
    public void initialize(Map<String, String> properties) {
        this.properties = properties;
        this.instanceId = properties.get(EnsureCloseClientsFactoryRegistry.FACTORY_INSTANCE_ID_KEY);
    }

    @Override
    public S3Client s3() {
        checkNotClosed();
        CloseableAwsClients local = s3Clients;
        if (local == null) {
            synchronized (this) {
                checkNotClosed();
                if (s3Clients == null) {
                    try {
                        Map<String, String> collectionProperties = IcebergUtils.filterCollectionProperties(properties);
                        s3Clients = S3Utils.buildClient(null, collectionProperties);
                        registerOnce();
                    } catch (CompilationException e) {
                        throw new RuntimeException(e);
                    }
                }
                local = s3Clients;
            }
        }
        return (S3Client) local.getConsumingClient();
    }

    @Override
    public GlueClient glue() {
        checkNotClosed();
        CloseableAwsClients local = glueClients;
        if (local == null) {
            synchronized (this) {
                checkNotClosed();
                if (glueClients == null) {
                    try {
                        glueClients = GlueUtils.buildClient(null, properties);
                        registerOnce();
                    } catch (CompilationException e) {
                        throw new RuntimeException(e);
                    }
                }
                local = glueClients;
            }
        }
        return (GlueClient) local.getConsumingClient();
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        AwsUtils.closeClients(s3Clients);
        AwsUtils.closeClients(glueClients);

        s3Clients = null;
        glueClients = null;

        if (registered) {
            EnsureCloseClientsFactoryRegistry.unregister(instanceId, this);
        }
    }

    private void registerOnce() {
        if (!registered) {
            EnsureCloseClientsFactoryRegistry.register(instanceId, this);
            registered = true;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException(getClass().getSimpleName() + " is closed");
        }
    }

    @Override
    public S3AsyncClient s3Async() {
        return null;
    }

    @Override
    public KmsClient kms() {
        return null;
    }

    @Override
    public DynamoDbClient dynamo() {
        return null;
    }
}