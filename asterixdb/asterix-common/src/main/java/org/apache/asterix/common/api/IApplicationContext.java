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
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IApplicationContext {

    StorageProperties getStorageProperties();

    TransactionProperties getTransactionProperties();

    CompilerProperties getCompilerProperties();

    MetadataProperties getMetadataProperties();

    ExternalProperties getExternalProperties();

    ActiveProperties getActiveProperties();

    BuildProperties getBuildProperties();

    ReplicationProperties getReplicationProperties();

    MessagingProperties getMessagingProperties();

    NodeProperties getNodeProperties();

    /**
     * @return the library manager which implements {@link org.apache.asterix.common.library.ILibraryManager}
     */
    ILibraryManager getLibraryManager();

    /**
     * @return the service context
     */
    IServiceContext getServiceContext();

    /**
     * @return a connected instance of {@link IHyracksClientConnection}
     * @throws HyracksDataException
     *             if connection couldn't be established to cluster controller
     */
    IHyracksClientConnection getHcc() throws HyracksDataException;

    /**
     * @return the cluster coordination service.
     */
    ICoordinationService getCoordinationService();

    IReceptionist getReceptionist();

    /**
     * @return the configuration validator
     */
    IConfigValidator getConfigValidator();

    /**
     * Returns the extension manager
     *
     * @return the extension manager instance
     */
    Object getExtensionManager();
}
