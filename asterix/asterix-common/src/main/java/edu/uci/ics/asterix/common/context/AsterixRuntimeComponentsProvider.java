package edu.uci.ics.asterix.common.context;

import edu.uci.ics.asterix.transaction.management.ioopcallbacks.LSMBTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.ioopcallbacks.LSMInvertedIndexIOOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import edu.uci.ics.asterix.transaction.management.opcallbacks.IndexOperationTracker;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public class AsterixRuntimeComponentsProvider implements IIndexLifecycleManagerProvider, IStorageManagerInterface,
        ILSMIOOperationSchedulerProvider, ILSMMergePolicyProvider, ILSMOperationTrackerFactory,
        ILSMIOOperationCallbackProvider {
    private static final long serialVersionUID = 1L;

    private final ILSMIOOperationCallbackFactory ioOpCallbackFactory;

    public static final AsterixRuntimeComponentsProvider LSMBTREE_PROVIDER = new AsterixRuntimeComponentsProvider(
            LSMBTreeIOOperationCallbackFactory.INSTANCE);
    public static final AsterixRuntimeComponentsProvider LSMRTREE_PROVIDER = new AsterixRuntimeComponentsProvider(
            LSMRTreeIOOperationCallbackFactory.INSTANCE);
    public static final AsterixRuntimeComponentsProvider LSMINVERTEDINDEX_PROVIDER = new AsterixRuntimeComponentsProvider(
            LSMInvertedIndexIOOperationCallbackFactory.INSTANCE);
    public static final AsterixRuntimeComponentsProvider NOINDEX_PROVIDER = new AsterixRuntimeComponentsProvider(null);

    private AsterixRuntimeComponentsProvider(ILSMIOOperationCallbackFactory ioOpCallbackFactory) {
        this.ioOpCallbackFactory = ioOpCallbackFactory;
    }

    @Override
    public ILSMOperationTracker createOperationTracker(ILSMIndex index) {
        return new IndexOperationTracker(index, ioOpCallbackFactory);
    }

    @Override
    public ILSMIOOperationCallback getIOOperationCallback(ILSMIndex index) {
        return ((IndexOperationTracker) index.getOperationTracker()).getIOOperationCallback();
    }

    @Override
    public ILSMIOOperationScheduler getIOScheduler(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getLSMIOScheduler();
    }

    @Override
    public ILSMMergePolicy getMergePolicy(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getLSMMergePolicy();
    }

    @Override
    public IBufferCache getBufferCache(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getBufferCache();
    }

    @Override
    public IFileMapProvider getFileMapProvider(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getFileMapManager();
    }

    @Override
    public ILocalResourceRepository getLocalResourceRepository(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getLocalResourceRepository();
    }

    @Override
    public IIndexLifecycleManager getLifecycleManager(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getIndexLifecycleManager();
    }

    @Override
    public ResourceIdFactory getResourceIdFactory(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getResourceIdFactory();
    }
}
