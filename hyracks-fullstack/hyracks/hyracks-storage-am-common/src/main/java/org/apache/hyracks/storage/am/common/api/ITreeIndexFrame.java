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

package org.apache.hyracks.storage.am.common.api;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

public interface ITreeIndexFrame {

    public static class Constants {
        /*
         * Storage version #. Change this if you alter any tree frame formats to stop
         * possible corruption from old versions reading new formats.
         */
        public static final int VERSION = 7;
        public static final int TUPLE_COUNT_OFFSET = 0;
        public static final int FREE_SPACE_OFFSET = TUPLE_COUNT_OFFSET + 4;
        public static final int LEVEL_OFFSET = FREE_SPACE_OFFSET + 4;
        public static final int RESERVED_HEADER_SIZE = LEVEL_OFFSET + 1;

        private Constants() {
        }
    }

    public void initBuffer(byte level);

    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException;

    public void insert(ITupleReference tuple, int tupleIndex);

    public FrameOpSpaceStatus hasSpaceUpdate(ITupleReference newTuple, int oldTupleIndex);

    public void update(ITupleReference newTuple, int oldTupleIndex, boolean inPlace);

    public void delete(ITupleReference tuple, int tupleIndex);

    // returns true if slots were modified, false otherwise
    public boolean compact();

    // returns true if compressed.
    public boolean compress() throws HyracksDataException;

    public int getTupleCount();

    public int getTupleOffset(int slotNum);

    public int getTotalFreeSpace();

    public void setPageLsn(long pageLsn);

    public long getPageLsn();

    public void setPage(ICachedPage page);

    public ICachedPage getPage();

    public ByteBuffer getBuffer();

    public int getMaxTupleSize(int pageSize);

    public int getBytesRequiredToWriteTuple(ITupleReference tuple);

    // for debugging
    public String printHeader();

    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException;

    public ISlotManager getSlotManager();

    // ATTENTION: in b-tree operations it may not always be possible to
    // determine whether an ICachedPage is a leaf or interior node
    // a compatible interior and leaf implementation MUST return identical
    // values when given the same ByteBuffer for the functions below
    public boolean isLeaf();

    public boolean isInterior();

    public byte getLevel();

    public void setLevel(byte level);

    public int getSlotSize();

    // for debugging
    public int getFreeSpaceOff();

    public void setFreeSpaceOff(int freeSpace);

    public ITreeIndexTupleWriter getTupleWriter();

    public int getPageHeaderSize();

    public ITreeIndexTupleReference createTupleReference();

    public void setMultiComparator(MultiComparator cmp);

    public ITupleReference getLeftmostTuple() throws HyracksDataException;

    public ITupleReference getRightmostTuple() throws HyracksDataException;
}
