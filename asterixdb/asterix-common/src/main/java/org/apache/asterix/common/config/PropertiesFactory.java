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
package org.apache.asterix.common.config;

import org.apache.asterix.common.api.IPropertiesFactory;

public class PropertiesFactory implements IPropertiesFactory {

    protected final PropertiesAccessor propertiesAccessor;

    public PropertiesFactory(PropertiesAccessor propertiesAccessor) {
        this.propertiesAccessor = propertiesAccessor;
    }

    @Override
    public StorageProperties newStorageProperties() {
        return new StorageProperties(propertiesAccessor);
    }

    @Override
    public TransactionProperties newTransactionProperties() {
        return new TransactionProperties(propertiesAccessor);
    }

    @Override
    public CompilerProperties newCompilerProperties() {
        return new CompilerProperties(propertiesAccessor);
    }

    @Override
    public MetadataProperties newMetadataProperties() {
        return new MetadataProperties(propertiesAccessor);
    }

    @Override
    public ExternalProperties newExternalProperties() {
        return new ExternalProperties(propertiesAccessor);
    }

    @Override
    public ActiveProperties newActiveProperties() {
        return new ActiveProperties(propertiesAccessor);
    }

    @Override
    public BuildProperties newBuildProperties() {
        return new BuildProperties(propertiesAccessor);
    }

    @Override
    public ReplicationProperties newReplicationProperties() {
        return new ReplicationProperties(propertiesAccessor);
    }

    @Override
    public MessagingProperties newMessagingProperties() {
        return new MessagingProperties(propertiesAccessor);
    }

    @Override
    public NodeProperties newNodeProperties() {
        return new NodeProperties(propertiesAccessor);
    }
}
