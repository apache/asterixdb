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
package org.apache.asterix.external.feed.dataflow;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayDeque;

import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@link FrameSpiller} is used with feeds when "spill.to.disk.on.congestion" is set to true. The spiller spills
 * excess tuples to disk if an operator
 * cannot process incoming data at its arrival rate. The maximum size of data (tuples) that can be spilled to disk is
 * configured using the property
 * "max.spill.size.on.disk"
 */
public class FrameSpiller {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final int FRAMES_PER_FILE = 1024;
    public static final double MAX_SPILL_USED_BEFORE_RESUME = 0.8;

    private final String fileNamePrefix;
    private final ArrayDeque<File> files = new ArrayDeque<>();
    private final VSizeFrame frame;
    private final int budget; // Max current frames in disk allowed
    private BufferedOutputStream bos; // Current output stream
    private BufferedInputStream bis; // Current input stream
    private File currentWriteFile; // Current write file
    private File currentReadFile; // Current read file
    private int currentWriteCount = 0; // Current file write count
    private int currentReadCount = 0; // Current file read count
    private int totalWriteCount = 0; // Total frames spilled
    private int totalReadCount = 0; // Total frames read
    private int fileCount = 0; // How many spill files?

    public FrameSpiller(IHyracksTaskContext ctx, String fileNamePrefix, long budgetInBytes)
            throws HyracksDataException {
        this.frame = new VSizeFrame(ctx);
        this.fileNamePrefix = fileNamePrefix;
        this.budget = (int) Math.min(budgetInBytes / ctx.getInitialFrameSize(), Integer.MAX_VALUE);
        if (budget <= 0) {
            throw new HyracksDataException("Invalid budget " + budgetInBytes + ". Budget must be larger than 0");
        }
    }

    public void open() throws HyracksDataException {
        try {
            this.currentWriteFile = StoragePathUtil.createFile(fileNamePrefix, fileCount++);
            this.currentReadFile = currentWriteFile;
            this.bos = new BufferedOutputStream(new FileOutputStream(currentWriteFile));
            this.bis = new BufferedInputStream(new FileInputStream(currentReadFile));
        } catch (Exception e) {
            LOGGER.fatal("Unable to create spill file", e);
            throw HyracksDataException.create(e);
        }
    }

    public boolean switchToMemory() {
        return totalWriteCount == totalReadCount;
    }

    public int remaining() {
        return totalWriteCount - totalReadCount;
    }

    public synchronized ByteBuffer next() throws HyracksDataException {
        frame.reset();
        if (totalReadCount == totalWriteCount) {
            return null;
        }
        try {
            if (currentReadFile == null) {
                if (!files.isEmpty()) {
                    currentReadFile = files.pop();
                    bis = new BufferedInputStream(new FileInputStream(currentReadFile));
                } else {
                    return null;
                }
            }
            // read first frame
            bis.read(frame.getBuffer().array(), 0, frame.getFrameSize());
            byte frameCount = frame.getBuffer().array()[0];
            if (frameCount > 1) {
                // expand the frame keeping existing data
                frame.ensureFrameSize(frame.getMinSize() * frameCount);
                bis.read(frame.getBuffer().array(), frame.getMinSize(), frame.getFrameSize() - frame.getMinSize());
            }
            currentReadCount++;
            totalReadCount++;
            if (currentReadCount >= FRAMES_PER_FILE) {
                currentReadCount = 0;
                // done with the file
                bis.close();
                Files.delete(currentReadFile.toPath());
                if (!files.isEmpty()) {
                    currentReadFile = files.pop();
                    bis = new BufferedInputStream(new FileInputStream(currentReadFile));
                } else {
                    currentReadFile = null;
                }
            }
            return frame.getBuffer();
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        } finally {
            synchronized (this) {
                notify();
            }
        }
    }

    public double usedBudget() {
        return (double) (totalWriteCount - totalReadCount) / (double) budget;
    }

    public synchronized boolean spill(ByteBuffer frame) throws HyracksDataException {
        try {
            if (totalWriteCount - totalReadCount >= budget) {
                return false;
            }
            currentWriteCount++;
            totalWriteCount++;
            bos.write(frame.array());
            bos.flush();
            if (currentWriteCount >= FRAMES_PER_FILE) {
                bos.close();
                currentWriteCount = 0;
                currentWriteFile = StoragePathUtil.createFile(fileNamePrefix, fileCount++);
                files.add(currentWriteFile);
                bos = new BufferedOutputStream(new FileOutputStream(currentWriteFile));
            }
            return true;
        } catch (IOException e) {
            close();
            throw HyracksDataException.create(e);
        }
    }

    public synchronized void close() {
        // Do proper cleanup
        if (bos != null) {
            try {
                bos.flush();
                bos.close();
            } catch (IOException e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        if (bis != null) {
            try {
                bis.close();
            } catch (IOException e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        if (currentReadFile != null) {
            try {
                Files.deleteIfExists(currentReadFile.toPath());
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
            currentReadFile = null;
        }
        while (!files.isEmpty()) {
            File file = files.pop();
            try {
                Files.deleteIfExists(file.toPath());
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        currentWriteCount = 0;
        currentReadCount = 0;
        totalWriteCount = 0;
        totalReadCount = 0;
    }
}
