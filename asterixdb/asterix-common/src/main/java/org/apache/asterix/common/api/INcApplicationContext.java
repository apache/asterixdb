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

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.concurrent.Executor;

import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.IReplicaManager;
import org.apache.asterix.common.transactions.IRecoveryManagerFactory;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.hyracks.util.cache.ICacheManager;

public interface INcApplicationContext extends IApplicationContext {

    IIOManager getIoManager();

    Executor getThreadExecutor();

    ITransactionSubsystem getTransactionSubsystem();

    void preStop() throws Exception;

    boolean isShuttingdown();

    ILSMIOOperationScheduler getLSMIOScheduler();

    ILSMMergePolicyFactory getMetadataMergePolicyFactory();

    IBufferCache getBufferCache();

    ILocalResourceRepository getLocalResourceRepository();

    IDatasetLifecycleManager getDatasetLifecycleManager();

    IDatasetMemoryManager getDatasetMemoryManager();

    IResourceIdFactory getResourceIdFactory();

    void initialize(IRecoveryManagerFactory recoveryManagerFactory, IReceptionistFactory receptionistFactory,
            IConfigValidatorFactory configValidatorFactory, boolean initialRun) throws IOException, AlgebricksException;

    void setShuttingdown(boolean b);

    void deinitialize() throws HyracksDataException;

    Object getActiveManager();

    IReplicationManager getReplicationManager();

    IReplicationChannel getReplicationChannel();

    /**
     * Exports the metadata node to the metadata RMI port.
     *
     * @throws RemoteException
     */
    void exportMetadataNodeStub() throws RemoteException;

    /**
     * Initializes the metadata node and bootstraps the metadata.
     *
     * @param newUniverse
     * @param partitionId
     * @throws Exception
     */
    void initializeMetadata(boolean newUniverse, int partitionId) throws Exception;

    /**
     * Unexports the metadata node from the RMI registry
     *
     * @throws RemoteException
     */
    void unexportMetadataNodeStub() throws RemoteException;

    /**
     * Binds the exported metadata node to the CC's distributed state.
     *
     * @throws RemoteException
     */
    void bindMetadataNodeStub(CcId ccId) throws RemoteException;

    /**
     * @return instance of {@link org.apache.asterix.common.context.IStorageComponentProvider}
     */
    IStorageComponentProvider getStorageComponentProvider();

    @Override
    INCServiceContext getServiceContext();

    IIndexCheckpointManagerProvider getIndexCheckpointManagerProvider();

    IReplicaManager getReplicaManager();

    long getMaxTxnId();

    IPersistedResourceRegistry getPersistedResourceRegistry();

    /**
     * Gets the cache manager of this {@link INcApplicationContext}
     *
     * @return the cache manager
     */
    ICacheManager getCacheManager();
}
