package edu.uci.ics.asterix.common.context;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushController;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMFlushControllerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ReferenceCountingOperationTracker;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public enum AsterixRuntimeComponentsProvider implements IIndexLifecycleManagerProvider, IStorageManagerInterface,
        ILSMIOOperationSchedulerProvider, ILSMFlushControllerProvider, ILSMMergePolicyProvider, ILSMOperationTrackerFactory {
    INSTANCE;

    @Override
    public ILSMOperationTracker createOperationTracker(ILSMIndex index) {
        // TODO: Replace this with the IndexOperationTracker, once the other transactional components are in place.
        return new ReferenceCountingOperationTracker(index);
    }
    
    @Override
    public ILSMFlushController getFlushController(IHyracksTaskContext ctx) {
        return ((AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext().getApplicationObject())
                .getFlushController();
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
