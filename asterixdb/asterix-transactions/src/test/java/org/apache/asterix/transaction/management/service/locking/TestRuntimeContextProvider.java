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
package org.apache.asterix.transaction.management.service.locking;

import static org.mockito.Mockito.mock;

import java.util.concurrent.Executors;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.api.ThreadExecutor;
import org.apache.asterix.common.transactions.IAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

class TestRuntimeContextProvider implements IAppRuntimeContextProvider {

    ThreadExecutor ate = new ThreadExecutor(Executors.defaultThreadFactory());
    IDatasetLifecycleManager dlcm = mock(IDatasetLifecycleManager.class);

    @Override
    public ThreadExecutor getThreadExecutor() {
        return ate;
    }

    @Override
    public IBufferCache getBufferCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITransactionSubsystem getTransactionSubsystem() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IDatasetLifecycleManager getDatasetLifecycleManager() {
        return dlcm;
    }

    @Override
    public double getBloomFilterFalsePositiveRate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILSMOperationTracker getLSMBTreeOperationTracker(int datasetID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILSMIOOperationScheduler getLSMIOScheduler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IIOManager getIOManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public INcApplicationContext getAppContext() {
        throw new UnsupportedOperationException();
    }
}
