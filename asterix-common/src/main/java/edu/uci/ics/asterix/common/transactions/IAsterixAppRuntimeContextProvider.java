package edu.uci.ics.asterix.common.transactions;

import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public interface IAsterixAppRuntimeContextProvider {

    public IBufferCache getBufferCache();

    public IFileMapProvider getFileMapManager();

    public ITransactionSubsystem getTransactionSubsystem();

    public IIndexLifecycleManager getIndexLifecycleManager();

    public double getBloomFilterFalsePositiveRate();

    public ILSMMergePolicy getLSMMergePolicy();

    public ILSMOperationTracker getLSMBTreeOperationTracker(int datasetID);

    public ILSMIOOperationCallbackProvider getLSMBTreeIOOperationCallbackProvider();

    public ILSMIOOperationCallbackProvider getLSMRTreeIOOperationCallbackProvider();

    public ILSMIOOperationCallbackProvider getLSMInvertedIndexIOOperationCallbackProvider();

    public ILSMIOOperationCallbackProvider getNoOpIOOperationCallbackProvider();

    public ILSMIOOperationScheduler getLSMIOScheduler();

    public ILocalResourceRepository getLocalResourceRepository();

    public ResourceIdFactory getResourceIdFactory();

    public IIOManager getIOManager();

    public IVirtualBufferCache getVirtualBufferCache(int datasetID);
}
