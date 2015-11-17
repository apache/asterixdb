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
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IFeedManager;
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
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.ResourceIdFactory;

public interface IAsterixAppRuntimeContext {

    public IIOManager getIOManager();

    public Executor getThreadExecutor();

    public ITransactionSubsystem getTransactionSubsystem();

    public boolean isShuttingdown();

    public ILSMIOOperationScheduler getLSMIOScheduler();

    public ILSMMergePolicyFactory getMetadataMergePolicyFactory();

    public int getMetaDataIODeviceId();

    public IBufferCache getBufferCache();

    public IFileMapProvider getFileMapManager();

    public ILocalResourceRepository getLocalResourceRepository();

    public IDatasetLifecycleManager getDatasetLifecycleManager();

    public ResourceIdFactory getResourceIdFactory();

    public ILSMOperationTracker getLSMBTreeOperationTracker(int datasetID);

    public void initialize(boolean initialRun) throws IOException, ACIDException, AsterixException;

    public void setShuttingdown(boolean b);

    public void deinitialize() throws HyracksDataException;

    public double getBloomFilterFalsePositiveRate();

    public List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID);

    public IFeedManager getFeedManager();

    public IRemoteRecoveryManager getRemoteRecoveryManager();

    public IReplicaResourcesManager getReplicaResourcesManager();

    public IReplicationManager getReplicationManager();

    public IReplicationChannel getReplicationChannel();

    public void initializeResourceIdFactory() throws HyracksDataException;
}
