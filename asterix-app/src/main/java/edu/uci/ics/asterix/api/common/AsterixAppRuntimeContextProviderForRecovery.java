package edu.uci.ics.asterix.api.common;

import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
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
    public ILSMMergePolicy getLSMMergePolicy() {
        return asterixAppRuntimeContext.getLSMMergePolicy();
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
    public ILSMOperationTrackerFactory getLSMBTreeOperationTrackerFactory(boolean isPrimary) {
        return asterixAppRuntimeContext.getLSMBTreeOperationTrackerFactory(isPrimary);
    }

    @Override
    public ILSMOperationTrackerFactory getLSMRTreeOperationTrackerFactory() {
        return asterixAppRuntimeContext.getLSMRTreeOperationTrackerFactory();
    }

    @Override
    public ILSMOperationTrackerFactory getLSMInvertedIndexOperationTrackerFactory() {
        return asterixAppRuntimeContext.getLSMInvertedIndexOperationTrackerFactory();
    }

    @Override
    public IVirtualBufferCache getVirtualBufferCache(int datasetID) {
        return asterixAppRuntimeContext.getVirtualBufferCache(datasetID);
    }

    @Override
    public ILSMIOOperationCallbackProvider getNoOpIOOperationCallbackProvider() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ILSMIOOperationCallbackProvider getLSMBTreeIOOperationCallbackProvider() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ILSMIOOperationCallbackProvider getLSMRTreeIOOperationCallbackProvider() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ILSMIOOperationCallbackProvider getLSMInvertedIndexIOOperationCallbackProvider() {
        // TODO Auto-generated method stub
        return null;
    }
}
