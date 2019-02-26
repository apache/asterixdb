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

package org.apache.hyracks.algebricks.runtime.operators.win;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;

final class WindowPartitionWriter {

    private final IHyracksTaskContext ctx;

    private final String fileNamePrefix;

    private final SourceLocation sourceLoc;

    private final IFrame[] writerFrames;

    private int writerFrameCount;

    private long writerFirstFrameId;

    private long writerLastFrameId;

    private RunFileWriter fileWriter;

    private final AbstractWindowPartitionReader partitionReader;

    WindowPartitionWriter(IHyracksTaskContext ctx, int memSizeInFrames, String fileNamePrefix,
            int readerPositionStoreSize, SourceLocation sourceLoc) throws HyracksDataException {
        this.ctx = ctx;
        this.fileNamePrefix = fileNamePrefix;
        this.sourceLoc = sourceLoc;
        partitionReader = readerPositionStoreSize < 1 ? new WindowPartitionForwardReader()
                : new WindowPartitionSeekableReader(readerPositionStoreSize);
        int writerFrameBudget = memSizeInFrames - partitionReader.getReservedFrameCount();
        if (writerFrameBudget < 1) {
            throw new IllegalArgumentException(String.valueOf(memSizeInFrames));
        }
        writerFrames = new IFrame[writerFrameBudget];
        // Allocate one writer frame here. Remaining frames will be allocated lazily while writing
        allocateFrames(writerFrames, 1);
        writerFirstFrameId = writerLastFrameId = -1;
    }

    void close() throws HyracksDataException {
        try {
            partitionReader.closeFileReader();
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    void reset() {
        writerFrameCount = 0;
        if (fileWriter != null) {
            fileWriter.rewind();
        }
    }

    void nextFrame(long frameId, ByteBuffer frameBuffer) throws HyracksDataException {
        if (frameId < 0) {
            throw new IllegalArgumentException(String.valueOf(frameId));
        }
        if (writerFrameCount == 0) {
            if (writerFirstFrameId != frameId) {
                copyToFrame(frameBuffer, writerFrames[0]);
                writerFirstFrameId = frameId;
            }
        } else if (writerFrameCount < writerFrames.length) {
            IFrame writerFrame = writerFrames[writerFrameCount];
            if (writerFrame == null) {
                writerFrames[writerFrameCount] = writerFrame = new VSizeFrame(ctx);
            }
            copyToFrame(frameBuffer, writerFrame);
        } else {
            if (fileWriter == null) {
                FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(fileNamePrefix);
                fileWriter = new RunFileWriter(file, ctx.getIoManager());
                fileWriter.open();
            }
            int pos = frameBuffer.position();
            frameBuffer.position(0);
            fileWriter.nextFrame(frameBuffer);
            frameBuffer.position(pos);
        }
        writerLastFrameId = frameId;
        writerFrameCount++;
    }

    WindowPartitionReader getReader() {
        return partitionReader;
    }

    private void allocateFrames(IFrame[] outFrames, int count) throws HyracksDataException {
        for (int i = 0; i < count; i++) {
            outFrames[i] = new VSizeFrame(ctx);
        }
    }

    private static void copyToFrame(ByteBuffer fromBuffer, IFrame toFrame) throws HyracksDataException {
        toFrame.ensureFrameSize(fromBuffer.capacity());
        int fromPosition = fromBuffer.position();
        FrameUtils.copyAndFlip(fromBuffer, toFrame.getBuffer());
        fromBuffer.position(fromPosition);
    }

    private static <T> void swap(T[] array1, int index1, T[] array2, int index2) {
        T item1 = array1[index1];
        array1[index1] = array2[index2];
        array2[index2] = item1;
    }

    private abstract class AbstractWindowPartitionReader implements WindowPartitionReader {

        int readerFrameIdx = -1;

        GeneratedRunFileReader fileReader;

        @Override
        public void open() throws HyracksDataException {
            if (readerFrameIdx >= 0) {
                throw new IllegalStateException(String.valueOf(readerFrameIdx));
            }
            readerFrameIdx = 0;

            if (writerFrameCount > writerFrames.length) {
                openFileReader();
            }
        }

        @Override
        public final void close() throws HyracksDataException {
            if (readerFrameIdx != writerFrameCount) {
                throw new IllegalStateException();
            }

            // closeImpl() must guarantee that first writer frame will contain content of the last partition frame
            closeImpl();
            writerFirstFrameId = writerLastFrameId;

            readerFrameIdx = -1;

            if (writerFrameCount > writerFrames.length) {
                closeFileReader();
            }
        }

        void openFileReader() throws HyracksDataException {
            if (fileReader != null) {
                throw new IllegalStateException();
            }
            fileReader = fileWriter.createReader();
            fileReader.open();
        }

        void closeFileReader() throws HyracksDataException {
            GeneratedRunFileReader r = fileReader;
            if (r != null) {
                fileReader = null;
                r.close();
            }
        }

        void readFromFileReader(IFrame outFrame) throws HyracksDataException {
            if (!fileReader.nextFrame(outFrame)) {
                throw HyracksDataException.create(ErrorCode.EOF, sourceLoc);
            }
        }

        @Override
        public final IFrame nextFrame(boolean primaryScan) throws HyracksDataException {
            if (readerFrameIdx < 0) {
                throw new IllegalStateException();
            }
            if (readerFrameIdx >= writerFrameCount) {
                throw HyracksDataException.create(ErrorCode.EOF, sourceLoc);
            }
            IFrame frame = nextFrameImpl(primaryScan);
            readerFrameIdx++;
            return frame;
        }

        abstract void closeImpl() throws HyracksDataException;

        abstract IFrame nextFrameImpl(boolean primaryScan) throws HyracksDataException;

        abstract int getReservedFrameCount();
    }

    private final class WindowPartitionForwardReader extends AbstractWindowPartitionReader {

        @Override
        IFrame nextFrameImpl(boolean primaryScan) throws HyracksDataException {
            if (!primaryScan) {
                throw new IllegalArgumentException();
            }
            if (readerFrameIdx < writerFrames.length) {
                return writerFrames[readerFrameIdx];
            } else {
                IFrame writerFrame0 = writerFrames[0];
                readFromFileReader(writerFrame0);
                return writerFrame0;
            }
        }

        @Override
        void closeImpl() {
            int endFrameIdx = readerFrameIdx - 1;
            if (endFrameIdx > 0 && endFrameIdx < writerFrames.length) {
                // last partition frame is in writerFrames -> make it the first one
                swap(writerFrames, 0, writerFrames, endFrameIdx);
            }
        }

        @Override
        public void savePosition(int slotNo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyPosition(int slotFrom, int slotTo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void restorePosition(int slotNo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rewind() {
            throw new UnsupportedOperationException();
        }

        @Override
        int getReservedFrameCount() {
            return 0;
        }
    }

    private final class WindowPartitionSeekableReader extends AbstractWindowPartitionReader {

        private final IFrame[] fileFrames;

        private final long[] fileFrameIdxs;

        private final long[] fileFrameSizes;

        private final long[] filePositionStore;

        private final int[] readerFrameIdxStore;

        private WindowPartitionSeekableReader(int positionStoreSize) throws HyracksDataException {
            fileFrames = new IFrame[2]; // run file frames: one for primary scan, another for non-primary
            allocateFrames(fileFrames, fileFrames.length);
            fileFrameIdxs = new long[fileFrames.length];
            fileFrameSizes = new long[fileFrames.length];
            filePositionStore = new long[positionStoreSize];
            readerFrameIdxStore = new int[positionStoreSize];
        }

        @Override
        public void open() throws HyracksDataException {
            super.open();
            Arrays.fill(fileFrameIdxs, -1);
            Arrays.fill(filePositionStore, -1);
            Arrays.fill(readerFrameIdxStore, -1);
        }

        @Override
        IFrame nextFrameImpl(boolean primaryScan) throws HyracksDataException {
            if (readerFrameIdx < writerFrames.length) {
                return writerFrames[readerFrameIdx];
            } else {
                int fileFrameSlot = primaryScan ? 0 : 1;
                IFrame fileFrameRef = fileFrames[fileFrameSlot];
                long filePosition = fileReader.position();
                if (readerFrameIdx == fileFrameIdxs[fileFrameSlot]) {
                    fileReader.seek(filePosition + fileFrameSizes[fileFrameSlot]);
                } else {
                    readFromFileReader(fileFrameRef);
                    fileFrameSizes[fileFrameSlot] = fileReader.position() - filePosition;
                    fileFrameIdxs[fileFrameSlot] = readerFrameIdx;
                }
                return fileFrameRef;
            }
        }

        @Override
        public void closeImpl() {
            int endFrameIdx = readerFrameIdx - 1;
            if (endFrameIdx >= writerFrames.length) {
                // last partition frame was in the run file -> get contents from the file frame
                swap(writerFrames, 0, fileFrames, 0);
            } else if (endFrameIdx > 0) {
                // last partition frame is in writerFrames -> make it the first one
                swap(writerFrames, 0, writerFrames, endFrameIdx);
            }
        }

        @Override
        public void savePosition(int slotNo) {
            readerFrameIdxStore[slotNo] = readerFrameIdx;
            filePositionStore[slotNo] = fileReader != null ? fileReader.position() : 0;
        }

        @Override
        public void copyPosition(int slotFrom, int slotTo) {
            readerFrameIdxStore[slotTo] = readerFrameIdxStore[slotFrom];
            filePositionStore[slotTo] = filePositionStore[slotFrom];
        }

        @Override
        public void restorePosition(int slotNo) {
            seek(readerFrameIdxStore[slotNo], filePositionStore[slotNo]);
        }

        @Override
        public void rewind() {
            seek(0, 0);
        }

        private void seek(int readerFrameIdx, long filePosition) {
            this.readerFrameIdx = readerFrameIdx;
            if (fileReader != null) {
                fileReader.seek(filePosition);
            }
        }

        @Override
        int getReservedFrameCount() {
            return fileFrames.length;
        }
    }
}
