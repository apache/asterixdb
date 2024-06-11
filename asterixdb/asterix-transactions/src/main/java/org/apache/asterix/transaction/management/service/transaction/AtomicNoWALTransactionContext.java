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
package org.apache.asterix.transaction.management.service.transaction;

import static org.apache.hyracks.util.ExitUtil.EC_FAILED_TO_ROLLBACK_ATOMIC_STATEMENT;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.dataflow.LSMIndexUtil;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.IndexCheckpoint;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.impls.FlushOperation;
import org.apache.hyracks.util.ExitUtil;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@ThreadSafe
public class AtomicNoWALTransactionContext extends AtomicTransactionContext {

    private static final Logger LOGGER = LogManager.getLogger();
    private final INcApplicationContext appCtx;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public AtomicNoWALTransactionContext(TxnId txnId, INcApplicationContext appCtx) {
        super(txnId);
        this.appCtx = appCtx;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        final int txnState = getTxnState();
        switch (txnState) {
            case ITransactionManager.ABORTED:
                deleteUncommittedRecords();
                break;
            case ITransactionManager.COMMITTED:
                ensureDurable();
                break;
            default:
                throw new IllegalStateException("invalid state in txn clean up: " + getTxnState());
        }
    }

    private void deleteUncommittedRecords() {
        for (ILSMOperationTracker opTrackerRef : modifiedIndexes) {
            PrimaryIndexOperationTracker primaryIndexOpTracker = (PrimaryIndexOperationTracker) opTrackerRef;
            try {
                primaryIndexOpTracker.abort();
            } catch (HyracksDataException e) {
                throw new ACIDException(e);
            }
        }
    }

    private void ensureDurable() {
        List<FlushOperation> flushes = new ArrayList<>();
        List<Integer> datasetIds = new ArrayList<>();
        Map<String, ILSMComponentId> resourceMap = new HashMap<>();
        LogRecord dummyLogRecord = new LogRecord();
        try {
            for (ILSMOperationTracker opTrackerRef : modifiedIndexes) {
                PrimaryIndexOperationTracker primaryIndexOpTracker = (PrimaryIndexOperationTracker) opTrackerRef;
                primaryIndexOpTracker.triggerScheduleFlush(dummyLogRecord);
                flushes.addAll(primaryIndexOpTracker.getScheduledFlushes());
                datasetIds.add(primaryIndexOpTracker.getDatasetInfo().getDatasetID());
                for (Map.Entry<String, FlushOperation> entry : primaryIndexOpTracker.getLastFlushOperation()
                        .entrySet()) {
                    resourceMap.put(entry.getKey(), entry.getValue().getFlushingComponent().getId());
                }
            }
            LSMIndexUtil.waitFor(flushes);
            persistLogFile(datasetIds, resourceMap);
        } catch (Exception e) {
            deleteUncommittedRecords();
            throw new ACIDException(e);
        }
        try {
            commit();
        } catch (HyracksDataException e) {
            try {
                rollback(resourceMap);
            } catch (Exception ex) {
                LOGGER.error("Error while rolling back atomic statement for {}, halting JVM", txnId);
                ExitUtil.halt(EC_FAILED_TO_ROLLBACK_ATOMIC_STATEMENT);
            }
        } finally {
            deleteLogFile();
        }
        enableMerge();
    }

    private void persistLogFile(List<Integer> datasetIds, Map<String, ILSMComponentId> resourceMap)
            throws HyracksDataException, JsonProcessingException {
        IIOManager ioManager = appCtx.getPersistenceIoManager();
        FileReference fref = ioManager.resolve(Paths.get(StorageConstants.METADATA_TXN_NOWAL_DIR_NAME,
                StorageConstants.PARTITION_DIR_PREFIX + StorageConstants.METADATA_PARTITION,
                String.format("%s.log", txnId)).toString());
        ioManager.overwrite(fref, OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(toJson(datasetIds, resourceMap)).getBytes());
    }

    private ObjectNode toJson(List<Integer> datasetIds, Map<String, ILSMComponentId> resourceMap) {
        ObjectNode jsonNode = OBJECT_MAPPER.createObjectNode();
        jsonNode.put("txnId", txnId.getId());
        jsonNode.putPOJO("datasetIds", datasetIds);
        jsonNode.put("nodeId", appCtx.getServiceContext().getNodeId());
        jsonNode.putPOJO("resourceMap", resourceMap);
        return jsonNode;
    }

    public void deleteLogFile() {
        IIOManager ioManager = appCtx.getPersistenceIoManager();
        try {
            FileReference fref = ioManager.resolve(Paths.get(StorageConstants.METADATA_TXN_NOWAL_DIR_NAME,
                    StorageConstants.PARTITION_DIR_PREFIX + StorageConstants.METADATA_PARTITION,
                    String.format("%s.log", txnId)).toString());
            ioManager.delete(fref);
        } catch (HyracksDataException e) {
            throw new ACIDException(e);
        }
    }

    private void commit() throws HyracksDataException {
        for (ILSMOperationTracker opTrackerRef : modifiedIndexes) {
            PrimaryIndexOperationTracker primaryIndexOpTracker = (PrimaryIndexOperationTracker) opTrackerRef;
            primaryIndexOpTracker.commit();
        }
    }

    private void enableMerge() {
        for (ILSMOperationTracker opTrackerRef : modifiedIndexes) {
            PrimaryIndexOperationTracker primaryIndexOpTracker = (PrimaryIndexOperationTracker) opTrackerRef;
            for (IndexInfo indexInfo : primaryIndexOpTracker.getDatasetInfo().getIndexes().values()) {
                if (indexInfo.getIndex().isPrimaryIndex()) {
                    try {
                        indexInfo.getIndex().getMergePolicy().diskComponentAdded(indexInfo.getIndex(), false);
                    } catch (HyracksDataException e) {
                        throw new ACIDException(e);
                    }
                }
            }
        }
    }

    public void rollback(Map<String, ILSMComponentId> resourceMap) {
        deleteUncommittedRecords();
        IDatasetLifecycleManager datasetLifecycleManager = appCtx.getDatasetLifecycleManager();
        IIndexCheckpointManagerProvider indexCheckpointManagerProvider =
                datasetLifecycleManager.getIndexCheckpointManagerProvider();
        resourceMap.forEach((k, v) -> {
            try {
                IIndexCheckpointManager checkpointManager =
                        indexCheckpointManagerProvider.get(ResourceReference.ofIndex(k));
                if (checkpointManager.getCheckpointCount() > 0) {
                    IndexCheckpoint checkpoint = checkpointManager.getLatest();
                    if (checkpoint.getLastComponentId() == v.getMaxId()) {
                        LOGGER.info("Removing checkpoint for resource {} for component id {}", k,
                                checkpoint.getLastComponentId());
                        checkpointManager.deleteLatest(v.getMaxId());
                    }
                }
            } catch (HyracksDataException e) {
                throw new ACIDException(e);
            }
        });
    }

    @Override
    public boolean hasWAL() {
        return false;
    }
}
