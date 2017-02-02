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

import org.apache.asterix.common.config.IPropertiesProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.replication.IRemoteRecoveryManager;
import org.apache.asterix.common.replication.IReplicaResourcesManager;
import org.apache.asterix.common.replication.IReplicationChannel;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;

public interface IAppRuntimeContext extends IPropertiesProvider {

    IIOManager getIOManager();

    Executor getThreadExecutor();

    ITransactionSubsystem getTransactionSubsystem();

    boolean isShuttingdown();

    ILSMIOOperationScheduler getLSMIOScheduler();

    ILSMMergePolicyFactory getMetadataMergePolicyFactory();

    IBufferCache getBufferCache();

    IFileMapProvider getFileMapManager();

    ILocalResourceRepository getLocalResourceRepository();

    IDatasetLifecycleManager getDatasetLifecycleManager();

    IResourceIdFactory getResourceIdFactory();

    ILSMOperationTracker getLSMBTreeOperationTracker(int datasetID);

    void initialize(boolean initialRun) throws IOException, ACIDException, AsterixException;

    void setShuttingdown(boolean b);

    void deinitialize() throws HyracksDataException;

    double getBloomFilterFalsePositiveRate();

    Object getActiveManager();

    IRemoteRecoveryManager getRemoteRecoveryManager();

    IReplicaResourcesManager getReplicaResourcesManager();

    IReplicationManager getReplicationManager();

    IReplicationChannel getReplicationChannel();

    ILibraryManager getLibraryManager();

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
     * @throws Exception
     */
    void initializeMetadata(boolean newUniverse) throws Exception;

    /**
     * Unexports the metadata node from the RMI registry
     *
     * @throws RemoteException
     */
    void unexportMetadataNodeStub() throws RemoteException;

    /**
     * @return instance of {@link org.apache.asterix.common.context.IStorageComponentProvider}
     */
    IStorageComponentProvider getStorageComponentProvider();
}
