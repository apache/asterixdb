package edu.uci.ics.asterix.common.context;

import edu.uci.ics.asterix.transaction.management.service.recovery.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
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
    public TransactionSubsystem getTransactionSubsystem() {
        return asterixAppRuntimeContext.getTransactionSubsystem();
    }

    @Override
    public IIndexLifecycleManager getIndexLifecycleManager() {
        return asterixAppRuntimeContext.getIndexLifecycleManager();
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
    public ILSMOperationTrackerFactory getLSMBTreeOperationTrackerFactory() {
        return asterixAppRuntimeContext.getLSMBTreeOperationTrackerFactory();
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
    public ILSMIOOperationCallbackProvider getLSMBTreeIOOperationCallbackProvider() {
        return asterixAppRuntimeContext.getLSMBTreeIOOperationCallbackProvider();
    }

    @Override
    public ILSMIOOperationCallbackProvider getLSMRTreeIOOperationCallbackProvider() {
        return asterixAppRuntimeContext.getLSMRTreeIOOperationCallbackProvider();
    }

    @Override
    public ILSMIOOperationCallbackProvider getLSMInvertedIndexIOOperationCallbackProvider() {
        return asterixAppRuntimeContext.getLSMInvertedIndexIOOperationCallbackProvider();
    }

    @Override
    public ILSMIOOperationCallbackProvider getNoOpIOOperationCallbackProvider() {
        return asterixAppRuntimeContext.getNoOpIOOperationCallbackProvider();
    }

}
