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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.Checkpoint;
import org.apache.asterix.common.transactions.CheckpointProperties;
import org.apache.asterix.common.transactions.ICheckpointManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An abstract implementation of {@link ICheckpointManager}.
 * The AbstractCheckpointManager contains the implementation of
 * the base operations on checkpoints such as persisting and deleting them.
 */
public abstract class AbstractCheckpointManager implements ICheckpointManager {

    private static final Logger LOGGER = Logger.getLogger(AbstractCheckpointManager.class.getName());
    private static final String CHECKPOINT_FILENAME_PREFIX = "checkpoint_";
    public static final long SHARP_CHECKPOINT_LSN = -1;
    private static final FilenameFilter filter = (File dir, String name) -> name.startsWith(CHECKPOINT_FILENAME_PREFIX);
    private final File checkpointDir;
    private final int historyToKeep;
    private final int lsnThreshold;
    private final int pollFrequency;
    protected final ITransactionSubsystem txnSubsystem;
    private CheckpointThread checkpointer;

    public AbstractCheckpointManager(ITransactionSubsystem txnSubsystem, CheckpointProperties checkpointProperties) {
        this.txnSubsystem = txnSubsystem;
        String checkpointDirPath = checkpointProperties.getCheckpointDirPath();
        if (!checkpointDirPath.endsWith(File.separator)) {
            checkpointDirPath += File.separator;
        }
        checkpointDir = new File(checkpointDirPath);
        // Create the checkpoint directory if missing
        if (!checkpointDir.exists()) {
            checkpointDir.mkdirs();
        }
        lsnThreshold = checkpointProperties.getLsnThreshold();
        pollFrequency = checkpointProperties.getPollFrequency();
        // We must keep at least the latest checkpoint
        historyToKeep = checkpointProperties.getHistoryToKeep() == 0 ? 1 : checkpointProperties.getHistoryToKeep();
    }

    @Override
    public Checkpoint getLatest() throws ACIDException {
        // Read all checkpointObjects from the existing checkpoint files
        File[] checkpoints = checkpointDir.listFiles(filter);
        if (checkpoints == null || checkpoints.length == 0) {
            return null;
        }
        List<Checkpoint> checkpointObjectList = new ArrayList<>();
        for (File file : checkpoints) {
            try {
                LOGGER.log(Level.WARNING, "Reading checkpoint file: " + file.getAbsolutePath());
                String jsonString = new String(Files.readAllBytes(Paths.get(file.getAbsolutePath())));
                checkpointObjectList.add(Checkpoint.fromJson(jsonString));
            } catch (IOException e) {
                // ignore corrupted checkpoint file
                LOGGER.log(Level.WARNING, "Failed to read checkpoint file: " + file.getAbsolutePath(), e);
                file.delete();
                LOGGER.log(Level.INFO, "Deleted corrupted checkpoint file: " + file.getAbsolutePath());
            }
        }
        /**
         * If all checkpoint files are corrupted, we have no option but to try to perform recovery.
         * We will forge a checkpoint that forces recovery to start from the beginning of the log.
         * This shouldn't happen unless a hardware corruption happens.
         */
        if (checkpointObjectList.isEmpty()) {
            LOGGER.severe("All checkpoint files are corrupted. Forcing recovery from the beginning of the log");
            checkpointObjectList.add(forgeForceRecoveryCheckpoint());
        }

        // Sort checkpointObjects in descending order by timeStamp to find out the most recent one.
        Collections.sort(checkpointObjectList);

        // Return the most recent one (the first one in sorted list)
        return checkpointObjectList.get(0);
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

    public Path getCheckpointPath(long checkpointTimestamp) {
        return Paths.get(checkpointDir.getAbsolutePath() + File.separator + CHECKPOINT_FILENAME_PREFIX + Long
                .toString(checkpointTimestamp));
    }

    protected void capture(long minMCTFirstLSN, boolean sharp) throws HyracksDataException {
        ILogManager logMgr = txnSubsystem.getLogManager();
        ITransactionManager txnMgr = txnSubsystem.getTransactionManager();
        Checkpoint checkpointObject = new Checkpoint(logMgr.getAppendLSN(), minMCTFirstLSN, txnMgr.getMaxJobId(),
                System.currentTimeMillis(), sharp, StorageConstants.VERSION);
        persist(checkpointObject);
        cleanup();
    }

    protected Checkpoint forgeForceRecoveryCheckpoint() {
        /**
         * By setting the checkpoint first LSN (low watermark) to Long.MIN_VALUE, the recovery manager will start from
         * the first available log.
         * We set the storage version to the current version. If there is a version mismatch, it will be detected
         * during recovery.
         */
        return new Checkpoint(Long.MIN_VALUE, Long.MIN_VALUE, Integer.MIN_VALUE, System.currentTimeMillis(), false,
                StorageConstants.VERSION);
    }

    private void persist(Checkpoint checkpoint) throws HyracksDataException {
        // Get checkpoint file path
        Path path = getCheckpointPath(checkpoint.getTimeStamp());
        // Write checkpoint file to disk
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(checkpoint.asJson());
            writer.flush();
        } catch (IOException e) {
            throw new HyracksDataException("Failed to write checkpoint to disk", e);
        }
    }

    private void cleanup() {
        File[] checkpointFiles = checkpointDir.listFiles(filter);
        // Sort the filenames lexicographically to keep the latest checkpoint history files.
        Arrays.sort(checkpointFiles);
        for (int i = 0; i < checkpointFiles.length - historyToKeep; i++) {
            if (!checkpointFiles[i].delete()) {
                LOGGER.warning("Could not delete checkpoint file at: " + checkpointFiles[i].getAbsolutePath());
            }
        }
    }
}