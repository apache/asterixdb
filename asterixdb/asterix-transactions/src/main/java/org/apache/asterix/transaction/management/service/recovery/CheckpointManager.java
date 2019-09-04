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
package org.apache.asterix.transaction.management.service.recovery;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.ioopcallbacks.LSMIOOperationCallback;
import org.apache.asterix.common.transactions.CheckpointProperties;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * An implementation of {@link ICheckpointManager} that defines the logic
 * of checkpoints.
 */
public class CheckpointManager extends AbstractCheckpointManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long NO_SECURED_LSN = -1L;
    private final long datasetCheckpointIntervalNanos;
    private final Map<TxnId, Long> securedLSNs;
    private boolean suspended = false;

    public CheckpointManager(ITransactionSubsystem txnSubsystem, CheckpointProperties checkpointProperties) {
        super(txnSubsystem, checkpointProperties);
        datasetCheckpointIntervalNanos = TimeUnit.SECONDS.toNanos(checkpointProperties.getDatasetCheckpointInterval());
        securedLSNs = new HashMap<>();
    }

    /**
     * Performs a sharp checkpoint. All datasets are flushed and all transaction
     * log files are deleted.
     */
    @Override
    public synchronized void doSharpCheckpoint() throws HyracksDataException {
        LOGGER.info("Starting sharp checkpoint...");
        final IDatasetLifecycleManager datasetLifecycleManager =
                txnSubsystem.getApplicationContext().getDatasetLifecycleManager();
        datasetLifecycleManager.flushAllDatasets();
        capture(SHARP_CHECKPOINT_LSN, true);
        txnSubsystem.getLogManager().renewLogFiles();
        LOGGER.info("Completed sharp checkpoint.");
    }

    /***
     * Attempts to perform a soft checkpoint at the specified {@code checkpointTargetLSN}.
     * If a checkpoint cannot be captured due to datasets having LSN < {@code checkpointTargetLSN},
     * an asynchronous flush is triggered on them. When a checkpoint is successful, all transaction
     * log files that end with LSN < {@code checkpointTargetLSN} are deleted.
     */
    @Override
    public synchronized long tryCheckpoint(long checkpointTargetLSN) throws HyracksDataException {
        LOGGER.info("Attemping soft checkpoint...");
        final long minSecuredLSN = getMinSecuredLSN();
        if (minSecuredLSN != NO_SECURED_LSN && checkpointTargetLSN >= minSecuredLSN) {
            return minSecuredLSN;
        }
        final long minFirstLSN = txnSubsystem.getRecoveryManager().getMinFirstLSN();
        boolean checkpointSucceeded = minFirstLSN >= checkpointTargetLSN;
        if (!checkpointSucceeded && !suspended) {
            // Flush datasets with indexes behind target checkpoint LSN
            final IDatasetLifecycleManager dlcm = txnSubsystem.getApplicationContext().getDatasetLifecycleManager();
            dlcm.asyncFlushMatchingIndexes(newLaggingDatasetPredicate(checkpointTargetLSN));
        }
        capture(minFirstLSN, false);
        if (checkpointSucceeded) {
            txnSubsystem.getLogManager().deleteOldLogFiles(minFirstLSN);
            LOGGER.info(String.format("soft checkpoint succeeded at LSN(%s)", minFirstLSN));
        }
        return minFirstLSN;
    }

    @Override
    public synchronized void secure(TxnId id) throws HyracksDataException {
        securedLSNs.put(id, txnSubsystem.getRecoveryManager().getMinFirstLSN());
    }

    @Override
    public synchronized void completed(TxnId id) {
        securedLSNs.remove(id);
    }

    @Override
    public synchronized void checkpointIdleDatasets() throws HyracksDataException {
        if (suspended) {
            return;
        }
        final IDatasetLifecycleManager dlcm = txnSubsystem.getApplicationContext().getDatasetLifecycleManager();
        dlcm.asyncFlushMatchingIndexes(newIdleDatasetPredicate());
    }

    @Override
    public synchronized void suspend() {
        suspended = true;
    }

    @Override
    public synchronized void resume() {
        suspended = false;
    }

    private synchronized long getMinSecuredLSN() {
        return securedLSNs.isEmpty() ? NO_SECURED_LSN : Collections.min(securedLSNs.values());
    }

    private Predicate<ILSMIndex> newIdleDatasetPredicate() {
        final long currentTime = System.nanoTime();
        return lsmIndex -> {
            if (lsmIndex.isPrimaryIndex()) {
                PrimaryIndexOperationTracker opTracker = (PrimaryIndexOperationTracker) lsmIndex.getOperationTracker();
                return currentTime - opTracker.getLastFlushTime() >= datasetCheckpointIntervalNanos;
            }
            return false;
        };
    }

    private Predicate<ILSMIndex> newLaggingDatasetPredicate(long checkpointTargetLSN) {
        return lsmIndex -> {
            final LSMIOOperationCallback ioCallback = (LSMIOOperationCallback) lsmIndex.getIOOperationCallback();
            return ioCallback.getPersistenceLsn() < checkpointTargetLSN;
        };
    }
}
