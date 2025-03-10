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
package org.apache.asterix.common.context;

import static org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentId.MIN_VALID_COMPONENT_ID;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.IIOBlockingOperation;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.asterix.common.storage.DatasetResourceReference;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.storage.StorageIOStats;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponent;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeLocalResource;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentIdGenerator;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentIdGenerator;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IRateLimiter;
import org.apache.hyracks.storage.common.buffercache.SleepRateLimiter;
import org.apache.hyracks.storage.common.disk.IDiskResourceCacheLockNotifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatasetLifecycleManager implements IDatasetLifecycleManager, ILifeCycleComponent {

    private static final Logger LOGGER = LogManager.getLogger();
    protected final Map<Integer, DatasetResource> datasets = new ConcurrentHashMap<>();
    private final StorageProperties storageProperties;
    protected final ILocalResourceRepository resourceRepository;
    private final IVirtualBufferCache vbc;
    protected final INCServiceContext serviceCtx;
    protected final IRecoveryManager recoveryMgr;
    private final ILogManager logManager;
    private final LogRecord waitLog;
    protected final IDiskResourceCacheLockNotifier lockNotifier;
    private volatile boolean stopped = false;
    private final IIndexCheckpointManagerProvider indexCheckpointManagerProvider;
    // all LSM-trees share the same virtual buffer cache list
    private final List<IVirtualBufferCache> vbcs;

    public DatasetLifecycleManager(INCServiceContext serviceCtx, StorageProperties storageProperties,
            ILocalResourceRepository resourceRepository, IRecoveryManager recoveryMgr, ILogManager logManager,
            IVirtualBufferCache vbc, IIndexCheckpointManagerProvider indexCheckpointManagerProvider,
            IDiskResourceCacheLockNotifier lockNotifier) {
        this.serviceCtx = serviceCtx;
        this.recoveryMgr = recoveryMgr;
        this.logManager = logManager;
        this.storageProperties = storageProperties;
        this.resourceRepository = resourceRepository;
        this.vbc = vbc;
        int numMemoryComponents = storageProperties.getMemoryComponentsNum();
        this.vbcs = new ArrayList<>(numMemoryComponents);
        for (int i = 0; i < numMemoryComponents; i++) {
            vbcs.add(vbc);
        }
        this.indexCheckpointManagerProvider = indexCheckpointManagerProvider;
        this.lockNotifier = lockNotifier;
        waitLog = new LogRecord();
        waitLog.setLogType(LogType.WAIT_FOR_FLUSHES);
        waitLog.computeAndSetLogSize();
    }

    @Override
    public synchronized ILSMIndex get(String resourcePath) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int datasetID = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);
        return getIndex(datasetID, resourceID);
    }

    @Override
    public synchronized ILSMIndex getIndex(int datasetID, long resourceID) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        DatasetResource datasetResource = datasets.get(datasetID);
        if (datasetResource == null) {
            return null;
        }
        return datasetResource.getIndex(resourceID);
    }

    @Override
    public synchronized void register(String resourcePath, IIndex index) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int did = getDIDfromResourcePath(resourcePath);
        LocalResource resource = resourceRepository.get(resourcePath);
        DatasetResource datasetResource = datasets.get(did);
        lockNotifier.onRegister(resource, index);
        if (datasetResource == null) {
            datasetResource = getDatasetLifecycle(did);
        }
        datasetResource.register(resource, (ILSMIndex) index);
    }

    protected int getDIDfromResourcePath(String resourcePath) throws HyracksDataException {
        LocalResource lr = resourceRepository.get(resourcePath);
        if (lr == null) {
            return -1;
        }
        return ((DatasetLocalResource) lr.getResource()).getDatasetId();
    }

    protected long getResourceIDfromResourcePath(String resourcePath) throws HyracksDataException {
        LocalResource lr = resourceRepository.get(resourcePath);
        if (lr == null) {
            return -1;
        }
        return lr.getId();
    }

    private DatasetLocalResource getDatasetLocalResource(String resourcePath) throws HyracksDataException {
        LocalResource lr = resourceRepository.get(resourcePath);
        if (lr == null) {
            return null;
        }
        return (DatasetLocalResource) lr.getResource();
    }

    @Override
    public synchronized void unregister(String resourcePath) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int did = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);
        DatasetResource dsr = datasets.get(did);
        IndexInfo iInfo = dsr == null ? null : dsr.getIndexInfo(resourceID);

        if (dsr == null || iInfo == null) {
            throw HyracksDataException.create(ErrorCode.INDEX_DOES_NOT_EXIST, resourcePath);
        }

        lockNotifier.onUnregister(resourceID);
        PrimaryIndexOperationTracker opTracker = dsr.getOpTracker(iInfo.getPartition());
        if (iInfo.getReferenceCount() != 0 || (opTracker != null && opTracker.getNumActiveOperations() != 0)) {
            if (LOGGER.isErrorEnabled()) {
                final String logMsg = String.format(
                        "Failed to drop in-use index %s. Ref count (%d), Operation tracker active ops (%d)",
                        resourcePath, iInfo.getReferenceCount(), opTracker.getNumActiveOperations());
                LOGGER.error(logMsg);
            }
            throw HyracksDataException.create(ErrorCode.CANNOT_DROP_IN_USE_INDEX,
                    StoragePathUtil.getIndexNameFromPath(resourcePath));
        }

        // TODO: use fine-grained counters, one for each index instead of a single counter per dataset.
        DatasetInfo dsInfo = dsr.getDatasetInfo();
        dsInfo.waitForIO();
        closeIndex(iInfo);
        dsInfo.removeIndex(resourceID);
        synchronized (dsInfo) {
            int referenceCount = dsInfo.getReferenceCount();
            boolean open = dsInfo.isOpen();
            boolean empty = dsInfo.getIndexes().isEmpty();
            if (referenceCount == 0 && open && empty && !dsInfo.isExternal()) {
                LOGGER.debug("removing dataset {} from cache", dsInfo.getDatasetID());
                removeDatasetFromCache(dsInfo.getDatasetID());
            } else {
                LOGGER.debug("keeping dataset {} in cache, ref count {}, open {}, indexes count: {}",
                        dsInfo.getDatasetID(), referenceCount, open, dsInfo.getIndexes().size());
            }
        }
    }

    @Override
    public synchronized void open(String resourcePath) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        DatasetLocalResource localResource = getDatasetLocalResource(resourcePath);
        if (localResource == null) {
            throw HyracksDataException.create(ErrorCode.INDEX_DOES_NOT_EXIST, resourcePath);
        }
        int did = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);

        lockNotifier.onOpen(resourceID);
        try {
            DatasetResource datasetResource = datasets.get(did);
            int partition = localResource.getPartition();
            if (shouldRecoverLazily(datasetResource, partition)) {
                performLocalRecovery(resourcePath, datasetResource, partition);
            } else {
                openResource(resourcePath, false);
            }
        } finally {
            lockNotifier.onClose(resourceID);
        }
    }

    private void performLocalRecovery(String resourcePath, DatasetResource datasetResource, int partition)
            throws HyracksDataException {
        LOGGER.debug("performing local recovery for dataset {} partition {}", datasetResource.getDatasetInfo(),
                partition);
        FileReference indexRootRef = StoragePathUtil.getIndexRootPath(serviceCtx.getIoManager(), resourcePath);
        Map<Long, LocalResource> resources = resourceRepository.getResources(r -> true, List.of(indexRootRef));

        List<ILSMIndex> indexes = new ArrayList<>();
        for (LocalResource resource : resources.values()) {
            if (shouldSkipResource(resource)) {
                continue;
            }

            ILSMIndex index = getOrCreateIndex(resource);
            boolean undoTouch = !resourcePath.equals(resource.getPath());
            openResource(resource.getPath(), undoTouch);
            indexes.add(index);
        }

        if (!indexes.isEmpty()) {
            recoveryMgr.recoverIndexes(indexes);
        }

        datasetResource.markRecovered(partition);
    }

    private boolean shouldSkipResource(LocalResource resource) {
        DatasetLocalResource lr = (DatasetLocalResource) resource.getResource();
        return MetadataIndexImmutableProperties.isMetadataDataset(lr.getDatasetId())
                || (lr.getResource() instanceof LSMBTreeLocalResource
                        && ((LSMBTreeLocalResource) lr.getResource()).isSecondaryNoIncrementalMaintenance());
    }

    private ILSMIndex getOrCreateIndex(LocalResource resource) throws HyracksDataException {
        ILSMIndex index = get(resource.getPath());
        if (index == null) {
            DatasetLocalResource lr = (DatasetLocalResource) resource.getResource();
            index = (ILSMIndex) lr.createInstance(serviceCtx);
            register(resource.getPath(), index);
        }
        return index;
    }

    private void openResource(String resourcePath, boolean undoTouch) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int did = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);

        // Notify first before opening a resource
        lockNotifier.onOpen(resourceID);

        DatasetResource dsr = datasets.get(did);
        DatasetInfo dsInfo = dsr.getDatasetInfo();
        if (dsInfo == null || !dsInfo.isRegistered()) {
            throw new HyracksDataException(
                    "Failed to open index with resource ID " + resourceID + " since it does not exist.");
        }

        IndexInfo iInfo = dsInfo.getIndexes().get(resourceID);
        if (iInfo == null) {
            throw new HyracksDataException(
                    "Failed to open index with resource ID " + resourceID + " since it does not exist.");
        }

        dsr.open(true);
        dsr.touch();
        boolean indexTouched = false;
        try {
            if (!iInfo.isOpen()) {
                ILSMOperationTracker opTracker = iInfo.getIndex().getOperationTracker();
                synchronized (opTracker) {
                    iInfo.getIndex().activate();
                }
                iInfo.setOpen(true);
            }
            iInfo.touch();
            indexTouched = true;
        } finally {
            if (undoTouch) {
                dsr.untouch();
                if (indexTouched) {
                    iInfo.untouch();
                }
                lockNotifier.onClose(resourceID);
            }
        }
    }

    private boolean shouldRecoverLazily(DatasetResource resource, int partition) {
        // Perform lazy recovery only if the following conditions are met:
        // 1. Lazy recovery is enabled.
        // 2. The resource does not belong to the Metadata dataverse.
        // 3. The partition is being accessed for the first time.
        return recoveryMgr.isLazyRecoveryEnabled()
                && !MetadataIndexImmutableProperties.isMetadataDataset(resource.getDatasetID())
                && !resource.isRecovered(partition);
    }

    public DatasetResource getDatasetLifecycle(int did) {
        DatasetResource dsr = datasets.get(did);
        if (dsr != null) {
            return dsr;
        }
        synchronized (datasets) {
            dsr = datasets.get(did);
            if (dsr == null) {
                DatasetInfo dsInfo = new DatasetInfo(did, logManager);
                dsr = new DatasetResource(dsInfo);
                datasets.put(did, dsr);
            }
            return dsr;
        }
    }

    @Override
    public DatasetInfo getDatasetInfo(int datasetID) {
        return getDatasetLifecycle(datasetID).getDatasetInfo();
    }

    @Override
    public synchronized void close(String resourcePath) throws HyracksDataException {
        DatasetResource dsr = null;
        IndexInfo iInfo = null;
        try {
            validateDatasetLifecycleManagerState();
            int did = getDIDfromResourcePath(resourcePath);
            long resourceID = getResourceIDfromResourcePath(resourcePath);
            dsr = datasets.get(did);
            if (dsr == null) {
                throw HyracksDataException.create(ErrorCode.NO_INDEX_FOUND_WITH_RESOURCE_ID, resourceID);
            }
            iInfo = dsr.getIndexInfo(resourceID);
            if (iInfo == null) {
                throw HyracksDataException.create(ErrorCode.NO_INDEX_FOUND_WITH_RESOURCE_ID, resourceID);
            }
            lockNotifier.onClose(resourceID);
        } finally {
            // Regardless of what exception is thrown in the try-block (e.g., line 279),
            // we have to un-touch the index and dataset.
            if (iInfo != null) {
                iInfo.untouch();
            }
            if (dsr != null) {
                dsr.untouch();
            }
        }
    }

    @Override
    public synchronized List<IIndex> getOpenResources() {
        List<IndexInfo> openIndexesInfo = getOpenIndexesInfo();
        List<IIndex> openIndexes = new ArrayList<>();
        for (IndexInfo iInfo : openIndexesInfo) {
            openIndexes.add(iInfo.getIndex());
        }
        return openIndexes;
    }

    @Override
    public synchronized List<IndexInfo> getOpenIndexesInfo() {
        List<IndexInfo> openIndexesInfo = new ArrayList<>();
        for (DatasetResource dsr : datasets.values()) {
            for (IndexInfo iInfo : dsr.getIndexes().values()) {
                if (iInfo.isOpen()) {
                    openIndexesInfo.add(iInfo);
                }
            }
        }
        return openIndexesInfo;
    }

    @Override
    public List<IVirtualBufferCache> getVirtualBufferCaches(int datasetID, int ioDeviceNum) {
        return vbcs;
    }

    private void removeDatasetFromCache(int datasetID) throws HyracksDataException {
        datasets.remove(datasetID);
    }

    @Override
    public synchronized PrimaryIndexOperationTracker getOperationTracker(int datasetId, int partition, String path) {
        DatasetResource dataset = getDatasetLifecycle(datasetId);
        PrimaryIndexOperationTracker opTracker = dataset.getOpTracker(partition);
        if (opTracker == null) {
            populateOpTrackerAndIdGenerator(dataset, partition, path);
            opTracker = dataset.getOpTracker(partition);
        }
        return opTracker;
    }

    @Override
    public synchronized ILSMComponentIdGenerator getComponentIdGenerator(int datasetId, int partition, String path) {
        DatasetResource dataset = datasets.get(datasetId);
        ILSMComponentIdGenerator generator = dataset.getComponentIdGenerator(partition);
        if (generator == null) {
            populateOpTrackerAndIdGenerator(dataset, partition, path);
            generator = dataset.getComponentIdGenerator(partition);
        }
        return generator;
    }

    @Override
    public synchronized IRateLimiter getRateLimiter(int datasetId, int partition, long writeRateLimit) {
        DatasetResource dataset = datasets.get(datasetId);
        IRateLimiter rateLimiter = dataset.getRateLimiter(partition);
        if (rateLimiter == null) {
            rateLimiter = populateRateLimiter(dataset, partition, writeRateLimit);
        }
        return rateLimiter;
    }

    @Override
    public synchronized boolean isRegistered(int datasetId) {
        return datasets.containsKey(datasetId);
    }

    private void populateOpTrackerAndIdGenerator(DatasetResource dataset, int partition, String path) {
        final long lastValidId = getDatasetLastValidComponentId(path);
        ILSMComponentIdGenerator idGenerator =
                new LSMComponentIdGenerator(storageProperties.getMemoryComponentsNum(), lastValidId);
        PrimaryIndexOperationTracker opTracker = new PrimaryIndexOperationTracker(dataset.getDatasetID(), partition,
                logManager, dataset.getDatasetInfo(), idGenerator, indexCheckpointManagerProvider);
        dataset.setPrimaryIndexOperationTracker(partition, opTracker);
        dataset.setIdGenerator(partition, idGenerator);
    }

    private IRateLimiter populateRateLimiter(DatasetResource dataset, int partition, long writeRateLimit) {
        IRateLimiter rateLimiter = SleepRateLimiter.create(writeRateLimit);
        dataset.setRateLimiter(partition, rateLimiter);
        return rateLimiter;
    }

    private void validateDatasetLifecycleManagerState() throws HyracksDataException {
        if (stopped) {
            throw new HyracksDataException(DatasetLifecycleManager.class.getSimpleName() + " was stopped.");
        }
    }

    @Override
    public void start() {
        // no op
    }

    @Override
    public synchronized void flushAllDatasets() throws HyracksDataException {
        flushAllDatasets(partition -> true);
    }

    @Override
    public synchronized void flushAllDatasets(IntPredicate partitions) throws HyracksDataException {
        for (DatasetResource dsr : datasets.values()) {
            if (dsr.getDatasetInfo().isOpen()) {
                flushDatasetOpenIndexes(dsr, partitions, false);
            }
        }
    }

    @Override
    public synchronized void flushDataset(int datasetId, boolean asyncFlush) throws HyracksDataException {
        DatasetResource dsr = datasets.get(datasetId);
        if (dsr != null) {
            flushDatasetOpenIndexes(dsr, p -> true, asyncFlush);
        }
    }

    @Override
    public synchronized void asyncFlushMatchingIndexes(Predicate<ILSMIndex> indexPredicate)
            throws HyracksDataException {
        for (DatasetResource dsr : datasets.values()) {
            for (PrimaryIndexOperationTracker opTracker : dsr.getOpTrackers()) {
                synchronized (opTracker) {
                    asyncFlush(dsr, opTracker, indexPredicate);
                }
            }
        }
    }

    private void asyncFlush(DatasetResource dsr, PrimaryIndexOperationTracker opTracker,
            Predicate<ILSMIndex> indexPredicate) throws HyracksDataException {
        final int partition = opTracker.getPartition();
        for (ILSMIndex lsmIndex : dsr.getDatasetInfo().getDatasetPartitionOpenIndexes(partition)) {
            LSMIOOperationCallback ioCallback = (LSMIOOperationCallback) lsmIndex.getIOOperationCallback();
            if (needsFlush(opTracker, lsmIndex, ioCallback) && indexPredicate.test(lsmIndex)) {
                LOGGER.info("Async flushing {}", opTracker);
                opTracker.setFlushOnExit(true);
                opTracker.flushIfNeeded();
                break;
            }
        }
    }

    /*
     * This method can only be called asynchronously safely if we're sure no modify operation
     * will take place until the flush is scheduled
     */
    private void flushDatasetOpenIndexes(DatasetResource dsr, IntPredicate partitions, boolean asyncFlush)
            throws HyracksDataException {
        DatasetInfo dsInfo = dsr.getDatasetInfo();
        if (!dsInfo.isOpen()) {
            throw new IllegalStateException("flushDatasetOpenIndexes is called on a dataset that is closed");
        }
        if (dsInfo.isExternal()) {
            // no memory components for external dataset
            return;
        }
        // ensure all in-flight flushes gets scheduled
        final boolean requiresWaitLog =
                dsInfo.getIndexes().values().stream().noneMatch(indexInfo -> indexInfo.getIndex().isAtomic());
        if (requiresWaitLog) {
            logManager.log(waitLog);
        }

        for (PrimaryIndexOperationTracker primaryOpTracker : dsr.getOpTrackers()) {
            if (!partitions.test(primaryOpTracker.getPartition())) {
                continue;
            }
            // flush each partition one by one
            int numActiveOperations = primaryOpTracker.getNumActiveOperations();
            if (numActiveOperations > 0) {
                throw new IllegalStateException("flushDatasetOpenIndexes is called on dataset " + dsInfo.getDatasetID()
                        + " with currently " + "active operations, count=" + numActiveOperations);
            }
            primaryOpTracker.setFlushOnExit(true);
            primaryOpTracker.flushIfNeeded();
        }
        // ensure requested flushes were scheduled
        if (requiresWaitLog) {
            logManager.log(waitLog);
        }
        if (!asyncFlush) {
            List<FlushOperation> flushes = new ArrayList<>();
            for (PrimaryIndexOperationTracker primaryOpTracker : dsr.getOpTrackers()) {
                if (!partitions.test(primaryOpTracker.getPartition())) {
                    continue;
                }
                flushes.addAll(primaryOpTracker.getScheduledFlushes());
            }
            LSMIndexUtil.waitFor(flushes);
        }
    }

    private void closeDataset(DatasetResource dsr) throws HyracksDataException {
        // First wait for any ongoing IO operations
        DatasetInfo dsInfo = dsr.getDatasetInfo();
        try {
            flushDatasetOpenIndexes(dsr, p -> true, false);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
        // wait for merges that were scheduled due to the above flush
        // ideally, we shouldn't need this since merges should still work.
        // They don't need a special memory budget but there is a problem
        // for some merge policies that need to access dataset info (correlated prefix)
        dsInfo.waitForIO();
        for (IndexInfo iInfo : dsInfo.getIndexes().values()) {
            closeIndex(iInfo);
        }
        removeDatasetFromCache(dsInfo.getDatasetID());
        dsInfo.setOpen(false);
    }

    @Override
    public synchronized void closeDatasets(Set<Integer> datasetsToClose) throws HyracksDataException {
        ArrayList<DatasetResource> openDatasets = new ArrayList<>(datasets.values());
        for (DatasetResource dsr : openDatasets) {
            if (dsr.isOpen() && datasetsToClose.contains(dsr.getDatasetID())) {
                closeDataset(dsr);
            }
        }
    }

    @Override
    public synchronized void closeAllDatasets() throws HyracksDataException {
        ArrayList<DatasetResource> openDatasets = new ArrayList<>(datasets.values());
        for (DatasetResource dsr : openDatasets) {
            if (dsr.isOpen()) {
                closeDataset(dsr);
            }
        }
    }

    @Override
    public synchronized void stop(boolean dumpState, OutputStream outputStream) throws IOException {
        if (stopped) {
            return;
        }
        if (dumpState) {
            dumpState(outputStream);
        }

        closeAllDatasets();

        datasets.clear();
        stopped = true;
    }

    @Override
    public void dumpState(OutputStream outputStream) throws IOException {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("Memory budget = %d%n", storageProperties.getMemoryComponentGlobalBudget()));
        long avaialbleMemory = storageProperties.getMemoryComponentGlobalBudget()
                - (long) vbc.getUsage() * storageProperties.getMemoryComponentPageSize();
        sb.append(String.format("Memory available = %d%n", avaialbleMemory));
        sb.append("\n");

        String dsHeaderFormat = "%-10s %-6s %-16s %-12s\n";
        String dsFormat = "%-10d %-6b %-16d %-12d\n";
        String idxHeaderFormat = "%-10s %-11s %-6s %-16s %-6s\n";
        String idxFormat = "%-10d %-11d %-6b %-16d %-6s\n";

        sb.append("[Datasets]\n");
        sb.append(String.format(dsHeaderFormat, "DatasetID", "Open", "Reference Count", "Last Access"));
        for (DatasetResource dsr : datasets.values()) {
            DatasetInfo dsInfo = dsr.getDatasetInfo();
            sb.append(String.format(dsFormat, dsInfo.getDatasetID(), dsInfo.isOpen(), dsInfo.getReferenceCount(),
                    dsInfo.getLastAccess()));
        }
        sb.append("\n");

        sb.append("[Indexes]\n");
        sb.append(String.format(idxHeaderFormat, "DatasetID", "ResourceID", "Open", "Reference Count", "Index"));
        for (DatasetResource dsr : datasets.values()) {
            DatasetInfo dsInfo = dsr.getDatasetInfo();
            dsInfo.getIndexes().forEach((key, iInfo) -> sb.append(String.format(idxFormat, dsInfo.getDatasetID(), key,
                    iInfo.isOpen(), iInfo.getReferenceCount(), iInfo.getIndex())));
        }
        outputStream.write(sb.toString().getBytes());
    }

    @Override
    public void flushDataset(IReplicationStrategy replicationStrategy, IntPredicate partitions)
            throws HyracksDataException {
        for (DatasetResource dsr : datasets.values()) {
            if (dsr.isOpen() && replicationStrategy.isMatch(dsr.getDatasetID())) {
                flushDatasetOpenIndexes(dsr, partitions, false);
            }
        }
    }

    @Override
    public void waitForIO(IReplicationStrategy replicationStrategy, int partition) throws HyracksDataException {
        for (DatasetResource dsr : datasets.values()) {
            if (dsr.isOpen() && replicationStrategy.isMatch(dsr.getDatasetID())) {
                // Do a simple wait without any operation
                dsr.getDatasetInfo().waitForIOAndPerform(partition, NoOpBlockingIOOperation.INSTANCE);
            }
        }
    }

    /**
     * Waits for all ongoing IO operations on all open datasets and atomically performs the provided {@code operation}
     * on each opened index before allowing any I/Os to go through.
     *
     * @param replicationStrategy replication strategy
     * @param partition           partition to perform the required operation against
     * @param operation           operation to perform
     */
    @Override
    public void waitForIOAndPerform(IReplicationStrategy replicationStrategy, int partition,
            IIOBlockingOperation operation) throws HyracksDataException {
        // Signal the operation will be performed
        operation.beforeOperation();

        for (DatasetResource dsr : datasets.values()) {
            if (dsr.isOpen() && replicationStrategy.isMatch(dsr.getDatasetID())) {
                // Wait for all I/Os and then perform the requested operation
                dsr.getDatasetInfo().waitForIOAndPerform(partition, operation);
            }
        }

        // Signal the operation has been performed
        operation.afterOperation();
    }

    @Override
    public StorageIOStats getDatasetsIOStats() {
        StorageIOStats stats = new StorageIOStats();
        for (DatasetResource dsr : datasets.values()) {
            stats.addPendingFlushes(dsr.getDatasetInfo().getPendingFlushes());
            stats.addPendingMerges(dsr.getDatasetInfo().getPendingMerges());
            stats.addPendingReplications(dsr.getDatasetInfo().getPendingReplications());
        }
        return stats;
    }

    //TODO refactor this method with unregister method
    @Override
    public synchronized void closeIfOpen(String resourcePath) throws HyracksDataException {
        validateDatasetLifecycleManagerState();
        int did = getDIDfromResourcePath(resourcePath);
        long resourceID = getResourceIDfromResourcePath(resourcePath);

        DatasetResource dsr = datasets.get(did);
        IndexInfo iInfo = dsr == null ? null : dsr.getIndexInfo(resourceID);

        if (dsr == null || iInfo == null) {
            return;
        }

        PrimaryIndexOperationTracker opTracker = dsr.getOpTracker(iInfo.getPartition());
        if (iInfo.getReferenceCount() != 0 || (opTracker != null && opTracker.getNumActiveOperations() != 0)) {
            if (LOGGER.isErrorEnabled()) {
                final String logMsg = String.format(
                        "Failed to drop in-use index %s. Ref count (%d), Operation tracker active ops (%d)",
                        resourcePath, iInfo.getReferenceCount(), opTracker.getNumActiveOperations());
                LOGGER.error(logMsg);
            }
            throw HyracksDataException.create(ErrorCode.CANNOT_DROP_IN_USE_INDEX,
                    StoragePathUtil.getIndexNameFromPath(resourcePath));
        }

        // TODO: use fine-grained counters, one for each index instead of a single counter per dataset.
        DatasetInfo dsInfo = dsr.getDatasetInfo();
        dsInfo.waitForIO();
        closeIndex(iInfo);
        dsInfo.removeIndex(resourceID);
        synchronized (dsInfo) {
            if (dsInfo.getReferenceCount() == 0 && dsInfo.isOpen() && dsInfo.getIndexes().isEmpty()
                    && !dsInfo.isExternal()) {
                removeDatasetFromCache(dsInfo.getDatasetID());
            }
        }
    }

    @Override
    public synchronized void closePartition(int partitionId) {
        for (DatasetResource ds : datasets.values()) {
            ds.removePartition(partitionId);
        }
    }

    private void closeIndex(IndexInfo indexInfo) throws HyracksDataException {
        if (indexInfo.isOpen()) {
            ILSMOperationTracker opTracker = indexInfo.getIndex().getOperationTracker();
            synchronized (opTracker) {
                indexInfo.getIndex().deactivate(false);
            }
            indexCheckpointManagerProvider.close(DatasetResourceReference.of(indexInfo.getLocalResource()));
            indexInfo.setOpen(false);
        }
    }

    private long getDatasetLastValidComponentId(String indexPath) {
        try {
            final ResourceReference indexRef = ResourceReference.ofIndex(indexPath);
            final ResourceReference primaryIndexRef = indexRef.getDatasetReference();
            final IIndexCheckpointManager indexCheckpointManager = indexCheckpointManagerProvider.get(primaryIndexRef);
            if (indexCheckpointManager.getCheckpointCount() > 0) {
                return Math.max(indexCheckpointManager.getLatest().getLastComponentId(), MIN_VALID_COMPONENT_ID);
            }
            return MIN_VALID_COMPONENT_ID;
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean needsFlush(PrimaryIndexOperationTracker opTracker, ILSMIndex lsmIndex,
            LSMIOOperationCallback ioCallback) throws HyracksDataException {
        return !(lsmIndex.isCurrentMutableComponentEmpty() || ioCallback.hasPendingFlush()
                || opTracker.isFlushLogCreated() || opTracker.isFlushOnExit());
    }

    @Override
    public IIndexCheckpointManagerProvider getIndexCheckpointManagerProvider() {
        return indexCheckpointManagerProvider;
    }
}
