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
import org.apache.asterix.common.cluster.IGlobalRecoveryManager;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.common.transactions.IResourceIdManager;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
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
     * @return the fault tolerance strategy in use for the cluster
     */
    IFaultToleranceStrategy getFaultToleranceStrategy();

    /**
     * @return the active lifecycle listener at Cluster controller
     */
    IJobLifecycleListener getActiveLifecycleListener();

    /**
     * @return a new instance of {@link IHyracksClientConnection}
     */
    IHyracksClientConnection getHcc();

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
     * Returns the extension manager
     *
     * @return the extension manager instance
     */
    Object getExtensionManager();
}
