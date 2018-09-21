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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IndexCheckpoint;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
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
    private final Path indexPath;

    public IndexCheckpointManager(Path indexPath) {
        this.indexPath = indexPath;
    }

    @Override
    public synchronized void init(long validComponentSequence, long lsn, long validComponentId)
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
        IndexCheckpoint firstCheckpoint = IndexCheckpoint.first(validComponentSequence, lsn, validComponentId);
        persist(firstCheckpoint);
    }

    @Override
    public synchronized void replicated(long componentSequence, long masterLsn, long componentId)
            throws HyracksDataException {
        final Long localLsn = getLatest().getMasterNodeFlushMap().get(masterLsn);
        if (localLsn == null) {
            throw new IllegalStateException("Component flushed before lsn mapping was received");
        }
        flushed(componentSequence, localLsn, componentId);
    }

    @Override
    public synchronized void flushed(long componentSequence, long lsn, long componentId) throws HyracksDataException {
        final IndexCheckpoint latest = getLatest();
        IndexCheckpoint nextCheckpoint = IndexCheckpoint.next(latest, lsn, componentSequence, componentId);
        persist(nextCheckpoint);
        deleteHistory(nextCheckpoint.getId(), HISTORY_CHECKPOINTS);
    }

    @Override
    public synchronized void masterFlush(long masterLsn, long localLsn) throws HyracksDataException {
        final IndexCheckpoint latest = getLatest();
        latest.getMasterNodeFlushMap().put(masterLsn, localLsn);
        final IndexCheckpoint next = IndexCheckpoint.next(latest, latest.getLowWatermark(),
                latest.getValidComponentSequence(), latest.getLastComponentId());
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
    public long getValidComponentSequence() throws HyracksDataException {
        return getLatest().getValidComponentSequence();
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
                    Arrays.toString(indexPath.toFile().listFiles()));
            throw new IllegalStateException("Couldn't find any checkpoints for resource: " + indexPath);
        }
        checkpoints.sort(Comparator.comparingLong(IndexCheckpoint::getId).reversed());
        return checkpoints.get(0);
    }

    @Override
    public synchronized void setLastComponentId(long componentId) throws HyracksDataException {
        final IndexCheckpoint latest = getLatest();
        final IndexCheckpoint next =
                IndexCheckpoint.next(latest, latest.getLowWatermark(), latest.getValidComponentSequence(), componentId);
        persist(next);
    }

    @Override
    public synchronized void advanceValidComponentSequence(long componentSequence) throws HyracksDataException {
        final IndexCheckpoint latest = getLatest();
        if (componentSequence > latest.getValidComponentSequence()) {
            final IndexCheckpoint next = IndexCheckpoint.next(latest, latest.getLowWatermark(), componentSequence,
                    latest.getLastComponentId());
            persist(next);
        }
    }

    private List<IndexCheckpoint> getCheckpoints() throws ClosedByInterruptException {
        List<IndexCheckpoint> checkpoints = new ArrayList<>();
        final File[] checkpointFiles = indexPath.toFile().listFiles(CHECKPOINT_FILE_FILTER);
        if (checkpointFiles != null) {
            for (File checkpointFile : checkpointFiles) {
                try {
                    checkpoints.add(read(checkpointFile.toPath()));
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
        final Path checkpointPath = getCheckpointPath(checkpoint);
        for (int i = 1; i <= MAX_CHECKPOINT_WRITE_ATTEMPTS; i++) {
            try {
                // clean up from previous write failure
                if (checkpointPath.toFile().exists()) {
                    Files.delete(checkpointPath);
                }
                try (BufferedWriter writer = Files.newBufferedWriter(checkpointPath)) {
                    writer.write(checkpoint.asJson());
                }
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

    private IndexCheckpoint read(Path checkpointPath) throws IOException {
        return IndexCheckpoint.fromJson(new String(Files.readAllBytes(checkpointPath)));
    }

    private void deleteHistory(long latestId, int historyToKeep) {
        try {
            final File[] checkpointFiles = indexPath.toFile().listFiles(CHECKPOINT_FILE_FILTER);
            if (checkpointFiles != null) {
                for (File checkpointFile : checkpointFiles) {
                    if (getCheckpointIdFromFileName(checkpointFile.toPath()) < (latestId - historyToKeep)) {
                        Files.delete(checkpointFile.toPath());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.warn(() -> "Couldn't delete history checkpoints at " + indexPath, e);
        }
    }

    private Path getCheckpointPath(IndexCheckpoint checkpoint) {
        return Paths.get(indexPath.toString(),
                StorageConstants.INDEX_CHECKPOINT_FILE_PREFIX + String.valueOf(checkpoint.getId()));
    }

    private long getCheckpointIdFromFileName(Path checkpointPath) {
        return Long.valueOf(checkpointPath.getFileName().toString()
                .substring(StorageConstants.INDEX_CHECKPOINT_FILE_PREFIX.length()));
    }
}
