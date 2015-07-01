/*
 * Copyright 2014 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.List;
import java.util.concurrent.Executors;

import edu.uci.ics.asterix.common.api.AsterixThreadExecutor;
import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

class TestRuntimeContextProvider implements IAsterixAppRuntimeContextProvider {

    AsterixThreadExecutor ate = new AsterixThreadExecutor(Executors.defaultThreadFactory());    
    IIndexLifecycleManager ilm = new IndexLifecycleManager();
    
    @Override
    public AsterixThreadExecutor getThreadExecutor() {
        return ate;
    }

    @Override
    public IBufferCache getBufferCache() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IFileMapProvider getFileMapManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITransactionSubsystem getTransactionSubsystem() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IIndexLifecycleManager getIndexLifecycleManager() {
        return ilm;
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
    public ResourceIdFactory getResourceIdFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IIOManager getIOManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IAsterixAppRuntimeContext getAppContext() {
        throw new UnsupportedOperationException();
    }

    static class IndexLifecycleManager implements IIndexLifecycleManager {

        @Override
        public IIndex getIndex(long resourceID) throws HyracksDataException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void register(long resourceID, IIndex index) throws HyracksDataException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unregister(long resourceID) throws HyracksDataException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open(long resourceID) throws HyracksDataException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close(long resourceID) throws HyracksDataException {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<IIndex> getOpenIndexes() {
            throw new UnsupportedOperationException();
        }
        
    }
}