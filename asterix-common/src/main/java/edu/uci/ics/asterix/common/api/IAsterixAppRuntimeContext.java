package edu.uci.ics.asterix.common.api;

import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationScheduler;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;
import edu.uci.ics.hyracks.storage.common.file.ILocalResourceRepository;
import edu.uci.ics.hyracks.storage.common.file.ResourceIdFactory;

public interface IAsterixAppRuntimeContext {

    public ITransactionSubsystem getTransactionSubsystem();

    public boolean isShuttingdown();

    public ILSMIOOperationScheduler getLSMIOScheduler();

    public ILSMMergePolicy getLSMMergePolicy();

    public IBufferCache getBufferCache();

    public IFileMapProvider getFileMapManager();

    public ILocalResourceRepository getLocalResourceRepository();

    public IIndexLifecycleManager getIndexLifecycleManager();

    public ResourceIdFactory getResourceIdFactory();

    public ILSMOperationTrackerFactory getLSMBTreeOperationTrackerFactory();

    public void initialize() throws IOException, ACIDException, AsterixException;

    public void setShuttingdown(boolean b);

    public void deinitialize() throws HyracksDataException;

    public double getBloomFilterFalsePositiveRate();

    public IVirtualBufferCache getVirtualBufferCache(int datasetID);
}
