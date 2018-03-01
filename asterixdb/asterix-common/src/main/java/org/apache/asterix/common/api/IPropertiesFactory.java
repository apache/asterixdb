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
package org.apache.asterix.common.api;

import org.apache.asterix.common.config.ActiveProperties;
import org.apache.asterix.common.config.BuildProperties;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.config.MessagingProperties;
import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.config.NodeProperties;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.config.TransactionProperties;

public interface IPropertiesFactory {

    /**
     * Creates new {@link StorageProperties}
     *
     * @return new storage properties
     */
    StorageProperties newStorageProperties();

    /**
     * Creates new {@link TransactionProperties}
     *
     * @return new transaction properties
     */
    TransactionProperties newTransactionProperties();

    /**
     * Creates new {@link CompilerProperties}
     *
     * @return new compiler properties
     */
    CompilerProperties newCompilerProperties();

    /**
     * Creates new {@link MetadataProperties}
     *
     * @return new metadata properties
     */
    MetadataProperties newMetadataProperties();

    /**
     * Creates new {@link ExternalProperties}
     *
     * @return new external properties
     */
    ExternalProperties newExternalProperties();

    /**
     * Creates new {@link ActiveProperties}
     *
     * @return new active properties
     */
    ActiveProperties newActiveProperties();

    /**
     * Creates new {@link BuildProperties}
     *
     * @return new build properties
     */
    BuildProperties newBuildProperties();

    /**
     * Creates new {@link ReplicationProperties}
     *
     * @return new replication properties
     */
    ReplicationProperties newReplicationProperties();

    /**
     * Creates new {@link MessagingProperties}
     *
     * @return new messaging properties
     */
    MessagingProperties newMessagingProperties();

    /**
     * Creates new {@link NodeProperties}
     *
     * @return new node properties
     */
    NodeProperties newNodeProperties();
}
