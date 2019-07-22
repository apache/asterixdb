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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.CheckpointProperties;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An abstract implementation of {@link ICheckpointManager}.
 * The AbstractCheckpointManager contains the implementation of
 * the base operations on checkpoints such as persisting and deleting them.
 */
public abstract class AbstractCheckpointManager implements ICheckpointManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final String CHECKPOINT_FILENAME_PREFIX = "checkpoint_";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    public static final long SHARP_CHECKPOINT_LSN = -1;
    private static final FilenameFilter filter = (File dir, String name) -> name.startsWith(CHECKPOINT_FILENAME_PREFIX);
    private static final long FIRST_CHECKPOINT_ID = 0;
    private final File checkpointDir;
    private final int historyToKeep;
    private final int lsnThreshold;
    private final int pollFrequency;
    private final IPersistedResourceRegistry persistedResourceRegistry;
    protected final ITransactionSubsystem txnSubsystem;
    private CheckpointThread checkpointer;

    public AbstractCheckpointManager(ITransactionSubsystem txnSubsystem, CheckpointProperties checkpointProperties) {
        this.txnSubsystem = txnSubsystem;
        String checkpointDirPath = checkpointProperties.getCheckpointDirPath();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.log(Level.INFO, "Checkpoint directory = " + checkpointDirPath);
        }
        if (!checkpointDirPath.endsWith(File.separator)) {
            checkpointDirPath += File.separator;
        }
        checkpointDir = new File(checkpointDirPath);
        // Create the checkpoint directory if missing
        if (!checkpointDir.exists()) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.log(Level.INFO, "Checkpoint directory " + checkpointDirPath + " didn't exist. Creating one");
            }
            checkpointDir.mkdirs();
        }
        lsnThreshold = checkpointProperties.getLsnThreshold();
        pollFrequency = checkpointProperties.getPollFrequency();
        // We must keep at least the latest checkpoint
        historyToKeep = checkpointProperties.getHistoryToKeep() + 1;
        persistedResourceRegistry = txnSubsystem.getApplicationContext().getPersistedResourceRegistry();
    }

    @Override
    public Checkpoint getLatest() {
        LOGGER.log(Level.INFO, "Getting latest checkpoint");
        final List<File> checkpointFiles = getCheckpointFiles();
        if (checkpointFiles.isEmpty()) {
            return null;
        }
        final List<Checkpoint> orderedCheckpoints = getOrderedValidCheckpoints(checkpointFiles, false);
        if (orderedCheckpoints.isEmpty()) {
            /*
             * If all checkpoint files are corrupted, we have no option but to try to perform recovery.
             * We will forge a checkpoint that forces recovery to start from the beginning of the log.
             * This shouldn't happen unless a hardware corruption happens.
             */
            return forgeForceRecoveryCheckpoint();
        }
        return orderedCheckpoints.get(orderedCheckpoints.size() - 1);
    }

    @Override
    public void start() {
        checkpointer = new CheckpointThread(this, txnSubsystem.getLogManager(), lsnThreshold, pollFrequency);
        checkpointer.start();
    }

    @Override
    public void stop(boolean dumpState, OutputStream ouputStream) throws IOException {
        checkpointer.shutdown();
        checkpointer.interrupt();
        try {
            // Wait until checkpoint thread stops
            checkpointer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void dumpState(OutputStream os) throws IOException {
        // Nothing to dump
    }

    public Path getCheckpointPath(long checkpointId) {
        return Paths.get(checkpointDir.getAbsolutePath() + File.separator + CHECKPOINT_FILENAME_PREFIX + checkpointId);
    }

    protected void capture(long minMCTFirstLSN, boolean sharp) throws HyracksDataException {
        ILogManager logMgr = txnSubsystem.getLogManager();
        ITransactionManager txnMgr = txnSubsystem.getTransactionManager();
        final long nextCheckpointId = getNextCheckpointId();
        final Checkpoint checkpointObject = new Checkpoint(nextCheckpointId, logMgr.getAppendLSN(), minMCTFirstLSN,
                txnMgr.getMaxTxnId(), sharp, StorageConstants.VERSION);
        persist(checkpointObject);
        cleanup();
    }

    private Checkpoint forgeForceRecoveryCheckpoint() {
        /*
         * By setting the checkpoint first LSN (low watermark) to Long.MIN_VALUE, the recovery manager will start from
         * the first available log.
         * We set the storage version to the current version. If there is a version mismatch, it will be detected
         * during recovery.
         */
        return new Checkpoint(Long.MIN_VALUE, Long.MIN_VALUE, Integer.MIN_VALUE, FIRST_CHECKPOINT_ID, false,
                StorageConstants.VERSION);
    }

    private void persist(Checkpoint checkpoint) throws HyracksDataException {
        // Get checkpoint file path
        Path path = getCheckpointPath(checkpoint.getId());

        if (LOGGER.isInfoEnabled()) {
            File file = path.toFile();
            LOGGER.log(Level.INFO, "Persisting checkpoint file to " + file + " which "
                    + (file.exists() ? "already exists" : "doesn't exist yet"));
        }
        // Write checkpoint file to disk
        try {
            byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(checkpoint.toJson(persistedResourceRegistry));
            Files.write(path, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            readCheckpoint(path);
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Failed to write checkpoint to disk", e);
            throw HyracksDataException.create(e);
        }
        if (LOGGER.isInfoEnabled()) {
            File file = path.toFile();
            LOGGER.log(Level.INFO, "Completed persisting checkpoint file to " + file + " which now "
                    + (file.exists() ? "exists" : " still doesn't exist"));
        }
    }

    private List<File> getCheckpointFiles() {
        File[] checkpoints = checkpointDir.listFiles(filter);
        if (checkpoints == null || checkpoints.length == 0) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.log(Level.INFO,
                        "Listing of files in the checkpoint dir returned " + (checkpoints == null ? "null" : "empty"));
            }
            return Collections.emptyList();
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.log(Level.INFO, "Listing of files in the checkpoint dir returned " + Arrays.toString(checkpoints));
        }
        return Arrays.asList(checkpoints);
    }

    private List<Checkpoint> getOrderedValidCheckpoints(List<File> checkpoints, boolean deleteCorrupted) {
        List<Checkpoint> checkpointObjectList = new ArrayList<>();
        for (File file : checkpoints) {
            try {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.log(Level.WARN, "Reading checkpoint file: " + file.getAbsolutePath());
                }
                Checkpoint cp = readCheckpoint(Paths.get(file.getAbsolutePath()));
                checkpointObjectList.add(cp);
            } catch (ClosedByInterruptException e) {
                Thread.currentThread().interrupt();
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.log(Level.WARN, "Interrupted while reading checkpoint file: " + file.getAbsolutePath(), e);
                }
                throw new ACIDException(e);
            } catch (IOException e) {
                // ignore corrupted checkpoint file
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.log(Level.WARN, "Failed to read checkpoint file: " + file.getAbsolutePath(), e);
                }
                if (deleteCorrupted && file.delete()) {
                    LOGGER.warn("Deleted corrupted checkpoint file: {}", file::getAbsolutePath);
                }
            }
        }
        Collections.sort(checkpointObjectList);
        return checkpointObjectList;
    }

    private void cleanup() {
        final List<File> checkpointFiles = getCheckpointFiles();
        final List<Checkpoint> orderedCheckpoints = getOrderedValidCheckpoints(checkpointFiles, true);
        final int deleteCount = orderedCheckpoints.size() - historyToKeep;
        for (int i = 0; i < deleteCount; i++) {
            final Checkpoint checkpoint = orderedCheckpoints.get(i);
            final Path checkpointPath = getCheckpointPath(checkpoint.getId());
            LOGGER.warn("Deleting checkpoint file at: {}", checkpointPath);
            if (!checkpointPath.toFile().delete()) {
                LOGGER.warn("Could not delete checkpoint file at: {}", checkpointPath);
            }
        }
    }

    private long getNextCheckpointId() {
        final List<File> checkpointFiles = getCheckpointFiles();
        if (checkpointFiles.isEmpty()) {
            return FIRST_CHECKPOINT_ID;
        }
        long maxOnDiskId = -1;
        for (File checkpointFile : checkpointFiles) {
            long fileId = Long.parseLong(checkpointFile.getName().substring(CHECKPOINT_FILENAME_PREFIX.length()));
            maxOnDiskId = Math.max(maxOnDiskId, fileId);
        }
        return maxOnDiskId + 1;
    }

    private Checkpoint readCheckpoint(Path checkpointPath) throws IOException {
        final JsonNode jsonNode = OBJECT_MAPPER.readValue(Files.readAllBytes(checkpointPath), JsonNode.class);
        return (Checkpoint) persistedResourceRegistry.deserialize(jsonNode);
    }
}
