/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.api.common;

import java.util.List;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public class AsterixAppRuntimeContextProviderForRecovery implements IAsterixAppRuntimeContextProvider {

    private final AsterixAppRuntimeContext asterixAppRuntimeContext;

    public AsterixAppRuntimeContextProviderForRecovery(AsterixAppRuntimeContext asterixAppRuntimeContext) {
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
}
