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
package org.apache.asterix.common.feeds;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class FeedFrameSpiller {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameSpiller.class.getName());

    private final IHyracksTaskContext ctx;
    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private final FeedPolicyAccessor policyAccessor;
    private BufferedOutputStream bos;
    private File file;
    private boolean fileCreated = false;
    private long bytesWritten = 0;
    private int spilledFrameCount = 0;

    public FeedFrameSpiller(IHyracksTaskContext ctx, FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            FeedPolicyAccessor policyAccessor) throws IOException {
        this.ctx = ctx;
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.policyAccessor = policyAccessor;
    }

    public boolean processMessage(ByteBuffer message) throws Exception {
        if (!fileCreated) {
            createFile();
            fileCreated = true;
        }
        long maxAllowed = policyAccessor.getMaxSpillOnDisk();
        if (maxAllowed != FeedPolicyAccessor.NO_LIMIT && bytesWritten + message.array().length > maxAllowed) {
            return false;
        } else {
            bos.write(message.array());
            bytesWritten += message.array().length;
            spilledFrameCount++;
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Spilled frame by " + runtimeId + " spill count " + spilledFrameCount);
            }
            return true;
        }
    }

    private void createFile() throws IOException {
        Date date = new Date();
        String dateSuffix = date.toString().replace(' ', '_');
        String fileName = connectionId.getFeedId() + "_" + connectionId.getDatasetName() + "_"
                + runtimeId.getFeedRuntimeType() + "_" + runtimeId.getPartition() + "_" + dateSuffix;

        file = new File(fileName);
        if (!file.exists()) {
            boolean success = file.createNewFile();
            if (!success) {
                throw new IOException("Unable to create spill file " + fileName + " for feed " + runtimeId);
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Created spill file " + file.getAbsolutePath());
                }
            }
        }
        bos = new BufferedOutputStream(new FileOutputStream(file));

    }

    public Iterator<ByteBuffer> replayData() throws Exception {
        bos.flush();
        return new FrameIterator(ctx, file.getName());
    }

    private static class FrameIterator implements Iterator<ByteBuffer> {

        private final BufferedInputStream bis;
        private final IHyracksTaskContext ctx;
        private int readFrameCount = 0;

        public FrameIterator(IHyracksTaskContext ctx, String filename) throws FileNotFoundException {
            bis = new BufferedInputStream(new FileInputStream(new File(filename)));
            this.ctx = ctx;
        }

        @Override
        public boolean hasNext() {
            boolean more = false;
            try {
                more = bis.available() > 0;
                if (!more) {
                    bis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            return more;
        }

        @Override
        public ByteBuffer next() {
            IFrame frame  = null;
            try {
                frame  = new VSizeFrame(ctx);
                Arrays.fill(frame.getBuffer().array(), (byte) 0);
                bis.read(frame.getBuffer().array(), 0, frame.getFrameSize());
                readFrameCount++;
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Read spill frome " + readFrameCount);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return frame.getBuffer();
        }

        @Override
        public void remove() {
        }

    }

    public void reset() {
        bytesWritten = 0;
        //  file.delete();
        fileCreated = false;
        bos = null;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Resetted the FrameSpiller!");
        }
    }

    public void close() {
        if (bos != null) {
            try {
                bos.flush();
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}