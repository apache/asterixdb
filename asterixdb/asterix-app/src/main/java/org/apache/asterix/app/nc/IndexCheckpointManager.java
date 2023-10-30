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
package org.apache.asterix.app.nc;

import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IndexCheckpoint;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOBulkOperation;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.util.annotations.ThreadSafe;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ThreadSafe
public class IndexCheckpointManager implements IIndexCheckpointManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final int HISTORY_CHECKPOINTS = 1;
    private static final int MAX_CHECKPOINT_WRITE_ATTEMPTS = 5;
    private static final FilenameFilter CHECKPOINT_FILE_FILTER =
            (file, name) -> name.startsWith(StorageConstants.INDEX_CHECKPOINT_FILE_PREFIX);
    private static final long BULKLOAD_LSN = 0;
    private final FileReference indexPath;
    private final IIOManager ioManager;

    public IndexCheckpointManager(FileReference indexPath, IIOManager ioManager) {
        this.indexPath = indexPath;
        this.ioManager = ioManager;
    }

    @Override
    public synchronized void init(long validComponentSequence, long lsn, long validComponentId, String masterNodeId)
            throws HyracksDataException {
        List<IndexCheckpoint> checkpoints;
        try {
            checkpoints = getCheckpoints();
        } catch (ClosedByInterruptException e) {
            throw HyracksDataException.create(e);
        }
        if (!checkpoints.isEmpty()) {
            LOGGER.warn(() -> "Checkpoints found on initializing: " + indexPath);
            delete();
        }
        IndexCheckpoint firstCheckpoint =
                IndexCheckpoint.first(validComponentSequence, lsn, validComponentId, masterNodeId);
        persist(firstCheckpoint);
    }

    @Override
    public synchronized void replicated(long componentSequence, long masterLsn, long componentId, String masterNodeId)
            throws HyracksDataException {
        final Long localLsn = getLatest().getMasterNodeFlushMap().get(masterLsn);
        if (localLsn == null) {
            throw new IllegalStateException("Component replicated before lsn mapping was received");
        }
        flushed(componentSequence, localLsn, componentId, masterNodeId);
    }

    @Override
    public synchronized void flushed(long componentSequence, long lsn, long componentId, String masterNodeId)
            throws HyracksDataException {
        final IndexCheckpoint latest = getLatest();
        IndexCheckpoint nextCheckpoint =
                IndexCheckpoint.next(latest, lsn, componentSequence, componentId, masterNodeId);
        persist(nextCheckpoint);
        deleteHistory(nextCheckpoint.getId(), HISTORY_CHECKPOINTS);
    }

    @Override
    public synchronized void flushed(long componentSequence, long lsn, long componentId) throws HyracksDataException {
        flushed(componentSequence, lsn, componentId, null);
    }

    @Override
    public synchronized void masterFlush(long masterLsn, long localLsn) throws HyracksDataException {
        final IndexCheckpoint latest = getLatest();
        latest.getMasterNodeFlushMap().put(masterLsn, localLsn);
        LOGGER.trace("index {} master flush {} -> {}", indexPath, masterLsn, localLsn);
        final IndexCheckpoint next = IndexCheckpoint.next(latest, latest.getLowWatermark(),
                latest.getValidComponentSequence(), latest.getLastComponentId(), null);
        persist(next);
        notifyAll();
    }

    @Override
    public synchronized long getLowWatermark() throws HyracksDataException {
        return getLatest().getLowWatermark();
    }

    @Override
    public synchronized boolean isFlushed(long masterLsn) throws HyracksDataException {
        if (masterLsn == BULKLOAD_LSN) {
            return true;
        }
        return getLatest().getMasterNodeFlushMap().containsKey(masterLsn);
    }

    @Override
    public synchronized void delete() {
        deleteHistory(Long.MAX_VALUE, 0);
    }

    @Override
    public synchronized boolean isValidIndex() throws HyracksDataException {
        return getCheckpointCount() > 0;
    }

    @Override
    public long getValidComponentSequence() throws HyracksDataException {
        if (getCheckpointCount() > 0) {
            return getLatest().getValidComponentSequence();
        }
        return AbstractLSMIndexFileManager.UNINITIALIZED_COMPONENT_SEQ;
    }

    @Override
    public int getCheckpointCount() throws HyracksDataException {
        try {
            return getCheckpoints().size();
        } catch (ClosedByInterruptException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public synchronized IndexCheckpoint getLatest() throws HyracksDataException {
        List<IndexCheckpoint> checkpoints;
        try {
            checkpoints = getCheckpoints();
        } catch (ClosedByInterruptException e) {
            throw HyracksDataException.create(e);
        }
        if (checkpoints.isEmpty()) {
            LOGGER.warn("Couldn't find any checkpoint file for index {}. Content of dir are {}.", indexPath,
                    ioManager.list(indexPath, IoUtil.NO_OP_FILTER).toString());
            throw new IllegalStateException("Couldn't find any checkpoints for resource: " + indexPath);
        }
        checkpoints.sort(Comparator.comparingLong(IndexCheckpoint::getId).reversed());
        return checkpoints.get(0);
    }

    @Override
    public synchronized void setLastComponentId(long componentId) throws HyracksDataException {
        final IndexCheckpoint latest = getLatest();
        final IndexCheckpoint next = IndexCheckpoint.next(latest, latest.getLowWatermark(),
                latest.getValidComponentSequence(), componentId, null);
        persist(next);
    }

    @Override
    public synchronized void advanceValidComponent(long componentSequence, long componentId)
            throws HyracksDataException {
        final IndexCheckpoint latest = getLatest();
        if (componentSequence > latest.getValidComponentSequence()) {
            final IndexCheckpoint next =
                    IndexCheckpoint.next(latest, latest.getLowWatermark(), componentSequence, componentId, null);
            persist(next);
        }
    }

    private List<IndexCheckpoint> getCheckpoints() throws ClosedByInterruptException, HyracksDataException {
        List<IndexCheckpoint> checkpoints = new ArrayList<>();
        final Collection<FileReference> checkpointFiles = ioManager.list(indexPath, CHECKPOINT_FILE_FILTER);
        if (!checkpointFiles.isEmpty()) {
            for (FileReference checkpointFile : checkpointFiles) {
                try {
                    checkpoints.add(read(checkpointFile));
                } catch (ClosedByInterruptException e) {
                    throw e;
                } catch (IOException e) {
                    LOGGER.warn(() -> "Couldn't read index checkpoint file: " + checkpointFile, e);
                }
            }
        }
        return checkpoints;
    }

    private void persist(IndexCheckpoint checkpoint) throws HyracksDataException {
        final FileReference checkpointPath = getCheckpointPath(checkpoint);
        for (int i = 1; i <= MAX_CHECKPOINT_WRITE_ATTEMPTS; i++) {
            try {
                // Overwrite will clean up from previous write failure (if any)
                ioManager.overwrite(checkpointPath, checkpoint.asJson().getBytes());
                // ensure it was written correctly by reading it
                read(checkpointPath);
                return;
            } catch (ClosedByInterruptException e) {
                LOGGER.info("interrupted while writing checkpoint at {}", checkpointPath);
                throw HyracksDataException.create(e);
            } catch (IOException e) {
                if (i == MAX_CHECKPOINT_WRITE_ATTEMPTS) {
                    throw HyracksDataException.create(e);
                }
                LOGGER.warn(() -> "Filed to write checkpoint at: " + indexPath, e);
                int nextAttempt = i + 1;
                LOGGER.info(() -> "Checkpoint write attempt " + nextAttempt + "/" + MAX_CHECKPOINT_WRITE_ATTEMPTS);
            }
        }
    }

    private IndexCheckpoint read(FileReference checkpointPath) throws IOException {
        return IndexCheckpoint.fromJson(new String(ioManager.readAllBytes(checkpointPath)));
    }

    @Override
    public void deleteLatest(long latestId, int historyToDelete) {
        try {
            final Collection<FileReference> checkpointFiles = ioManager.list(indexPath, CHECKPOINT_FILE_FILTER);
            if (!checkpointFiles.isEmpty()) {
                for (FileReference checkpointFile : checkpointFiles) {
                    if (getCheckpointIdFromFileName(checkpointFile) > (latestId - historyToDelete)) {
                        ioManager.delete(checkpointFile);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn(() -> "Couldn't delete history checkpoints at " + indexPath, e);
        }
    }

    private void deleteHistory(long latestId, int historyToKeep) {
        try {
            final Collection<FileReference> checkpointFiles = ioManager.list(indexPath, CHECKPOINT_FILE_FILTER);
            if (!checkpointFiles.isEmpty()) {
                IIOBulkOperation deleteBulk = ioManager.createDeleteBulkOperation();
                for (FileReference checkpointFile : checkpointFiles) {
                    if (getCheckpointIdFromFileName(checkpointFile) < (latestId - historyToKeep)) {
                        deleteBulk.add(checkpointFile);
                    }
                }
                ioManager.performBulkOperation(deleteBulk);
            }
        } catch (Exception e) {
            LOGGER.warn(() -> "Couldn't delete history checkpoints at " + indexPath, e);
        }
    }

    private FileReference getCheckpointPath(IndexCheckpoint checkpoint) {
        return indexPath.getChild(StorageConstants.INDEX_CHECKPOINT_FILE_PREFIX + checkpoint.getId());
    }

    private long getCheckpointIdFromFileName(FileReference checkpointPath) {
        return Long
                .parseLong(checkpointPath.getName().substring(StorageConstants.INDEX_CHECKPOINT_FILE_PREFIX.length()));
    }
}
