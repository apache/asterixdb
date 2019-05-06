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
package org.apache.asterix.common.dataflow;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.api.INodeJobTracker;
import org.apache.asterix.common.api.IRequestTracker;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.config.ExtensionProperties;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.metadata.IMetadataBootstrap;
import org.apache.asterix.common.replication.INcLifecycleCoordinator;
import org.apache.asterix.common.storage.ICompressionManager;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.asterix.common.transactions.ITxnIdFactory;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.job.IJobLifecycleListener;
import org.apache.hyracks.storage.common.IStorageManager;

/**
 * Provides methods for obtaining
 * {@link org.apache.hyracks.storage.common.IStorageManager},
 * {@link org.apache.hyracks.api.application.ICCServiceContext},
 * {@link org.apache.asterix.common.cluster.IGlobalRecoveryManager},
 * {@link org.apache.asterix.common.library.ILibraryManager},
 * {@link org.apache.asterix.common.transactions.IResourceIdManager}
 *
 * at the cluster controller side.
 */
public interface ICcApplicationContext extends IApplicationContext {

    /**
     * @return an instance which implements {@link org.apache.hyracks.storage.common.IStorageManager}
     */
    IStorageManager getStorageManager();

    /**
     * @return an instance which implements {@link org.apache.hyracks.api.application.ICCServiceContext}
     */
    @Override
    ICCServiceContext getServiceContext();

    /**
     * @return the global recovery manager which implements
     *         {@link org.apache.asterix.common.cluster.IGlobalRecoveryManager}
     */
    IGlobalRecoveryManager getGlobalRecoveryManager();

    /**
     * @return the NC lifecycle coordinator in use for the cluster
     */
    INcLifecycleCoordinator getNcLifecycleCoordinator();

    /**
     * @return the active notification handler at Cluster controller
     */
    IJobLifecycleListener getActiveNotificationHandler();

    /**
     * @return the cluster wide resource id manager
     */
    IResourceIdManager getResourceIdManager();

    /**
     * Returns the storage component provider
     *
     * @return {@link IStorageComponentProvider} implementation instance
     */
    IStorageComponentProvider getStorageComponentProvider();

    /**
     * @return the metadata lock manager
     */
    IMetadataLockManager getMetadataLockManager();

    /**
     * @return the metadata bootstrap
     */
    IMetadataBootstrap getMetadataBootstrap();

    /**
     * @return the cluster state manager
     */
    IClusterStateManager getClusterStateManager();

    /**
     * @return the extension properties
     */
    ExtensionProperties getExtensionProperties();

    /**
     * @return the node job tracker
     */
    INodeJobTracker getNodeJobTracker();

    /**
     * @return the transaction id factory
     */
    ITxnIdFactory getTxnIdFactory();

    /**
     * @return the compression manager
     */
    ICompressionManager getCompressionManager();

    /**
     * Gets the request tracker.
     *
     * @return the request tracker.
     */
    IRequestTracker getRequestTracker();
}
