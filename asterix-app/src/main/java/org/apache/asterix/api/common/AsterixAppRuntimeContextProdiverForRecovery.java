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
package org.apache.asterix.api.common;

import java.util.List;

import org.apache.asterix.common.api.AsterixThreadExecutor;
import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.ResourceIdFactory;

public class AsterixAppRuntimeContextProdiverForRecovery implements IAsterixAppRuntimeContextProvider {

    private final AsterixAppRuntimeContext asterixAppRuntimeContext;

    public AsterixAppRuntimeContextProdiverForRecovery(AsterixAppRuntimeContext asterixAppRuntimeContext) {
        this.asterixAppRuntimeContext = asterixAppRuntimeContext;
    }

    @Override
    public IBufferCache getBufferCache() {
        return asterixAppRuntimeContext.getBufferCache();
    }

    @Override
    public IFileMapProvider getFileMapManager() {
        return asterixAppRuntimeContext.getFileMapManager();
    }

    @Override
    public ITransactionSubsystem getTransactionSubsystem() {
        return asterixAppRuntimeContext.getTransactionSubsystem();
    }

    @Override
    public IIndexLifecycleManager getIndexLifecycleManager() {
        return asterixAppRuntimeContext.getIndexLifecycleManager();
    }

    @Override
    public double getBloomFilterFalsePositiveRate() {
        return asterixAppRuntimeContext.getBloomFilterFalsePositiveRate();
    }

    @Override
    public ILSMIOOperationScheduler getLSMIOScheduler() {
        return asterixAppRuntimeContext.getLSMIOScheduler();
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository() {
        return asterixAppRuntimeContext.getLocalResourceRepository();
    }

    @Override
    public ResourceIdFactory getResourceIdFactory() {
        return asterixAppRuntimeContext.getResourceIdFactory();
    }

    @Override
    public IIOManager getIOManager() {
        return asterixAppRuntimeContext.getIOManager();
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID) {
        return asterixAppRuntimeContext.getVirtualBufferCaches(datasetID);
    }

    @Override
    public ILSMOperationTracker getLSMBTreeOperationTracker(int datasetID) {
        return asterixAppRuntimeContext.getLSMBTreeOperationTracker(datasetID);
    }

    @Override
    public IAsterixAppRuntimeContext getAppContext() {
        return asterixAppRuntimeContext;
    }

    @Override
    public AsterixThreadExecutor getThreadExecutor() {
        return asterixAppRuntimeContext.getThreadExecutor();
    }
}
