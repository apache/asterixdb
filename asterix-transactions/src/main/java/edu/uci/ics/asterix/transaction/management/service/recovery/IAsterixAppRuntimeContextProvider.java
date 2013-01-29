package edu.uci.ics.asterix.transaction.management.service.recovery;

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

public interface IAsterixAppRuntimeContextProvider {

    public IBufferCache getBufferCache();

    public IFileMapProvider getFileMapManager();

    public TransactionSubsystem getTransactionSubsystem();

    public IIndexLifecycleManager getIndexLifecycleManager();

    public ILSMMergePolicy getLSMMergePolicy();

    public ILSMOperationTrackerFactory getLSMBTreeOperationTrackerFactory();
    
    public ILSMOperationTrackerFactory getLSMRTreeOperationTrackerFactory();
    
    public ILSMOperationTrackerFactory getLSMInvertedIndexOperationTrackerFactory();
    
    public ILSMIOOperationCallbackProvider getLSMBTreeIOOperationCallbackProvider();
    
    public ILSMIOOperationCallbackProvider getLSMRTreeIOOperationCallbackProvider();
    
    public ILSMIOOperationCallbackProvider getLSMInvertedIndexIOOperationCallbackProvider();
    
    public ILSMIOOperationCallbackProvider getNoOpIOOperationCallbackProvider();
    
    public ILSMIOOperationScheduler getLSMIOScheduler();

    public ILocalResourceRepository getLocalResourceRepository();

    public ResourceIdFactory getResourceIdFactory();
    
    public IIOManager getIOManager();
}
