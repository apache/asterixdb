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

package org.apache.hyracks.storage.am.btree.frames;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.btree.api.IPrefixSlotManager;
import org.apache.hyracks.storage.am.btree.compressors.FieldPrefixCompressor;
import org.apache.hyracks.storage.am.btree.impls.BTreeFieldPrefixTupleReference;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import org.apache.hyracks.storage.am.btree.impls.FieldPrefixPrefixTupleReference;
import org.apache.hyracks.storage.am.btree.impls.FieldPrefixSlotManager;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriter;
import org.apache.hyracks.storage.am.common.api.IBTreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameCompressor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleMode;
import org.apache.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import org.apache.hyracks.storage.am.common.ophelpers.SlotOffTupleOff;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IExtraPageBlockHelper;

public class BTreeFieldPrefixNSMLeafFrame implements IBTreeLeafFrame {

    protected static final int PAGE_LSN_OFFSET = ITreeIndexFrame.Constants.RESERVED_HEADER_SIZE;
    protected static final int TOTAL_FREE_SPACE_OFFSET = PAGE_LSN_OFFSET + 8;
    protected static final int SM_FLAG_OFFSET = TOTAL_FREE_SPACE_OFFSET + 4;
    protected static final int UNCOMPRESSED_TUPLE_COUNT_OFFSET = SM_FLAG_OFFSET + 1;
    protected static final int PREFIX_TUPLE_COUNT_OFFSET = UNCOMPRESSED_TUPLE_COUNT_OFFSET + 4;
    protected static final int NEXT_LEAF_OFFSET = PREFIX_TUPLE_COUNT_OFFSET + 4;

    private final IPrefixSlotManager slotManager;
    private final ITreeIndexFrameCompressor compressor;
    private final BTreeFieldPrefixTupleReference frameTuple;
    private final FieldPrefixPrefixTupleReference framePrefixTuple;
    private final BTreeTypeAwareTupleWriter tupleWriter;

    private MultiComparator cmp;

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;

    public BTreeFieldPrefixNSMLeafFrame(BTreeTypeAwareTupleWriter tupleWriter) {
        this.tupleWriter = tupleWriter;
        this.frameTuple = new BTreeFieldPrefixTupleReference(tupleWriter.createTupleReference());
        this.slotManager = new FieldPrefixSlotManager();

        ITypeTraits[] typeTraits = tupleWriter.getTypeTraits();
        this.framePrefixTuple = new FieldPrefixPrefixTupleReference(typeTraits);
        this.compressor = new FieldPrefixCompressor(typeTraits, 0.001f, 2);
    }

    @Override
    public int getMaxTupleSize(int pageSize) {
        return (pageSize - getPageHeaderSize()) / 2;
    }

    @Override
    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + slotManager.getSlotSize();
    }

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();
        slotManager.setFrame(this);
    }

    @Override
    public ByteBuffer getBuffer() {
        return page.getBuffer();
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    @Override
    public boolean compress() throws HyracksDataException {
        try {
            return compressor.compress(this, cmp);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    // Assumptions:
    // 1) prefix tuples are stored contiguously
    // 2) prefix tuples are located before tuples (physically on the page)
    // 3) prefix tuples are sorted (last prefix tuple is at highest offset)
    // This procedure will not move prefix tuples.
    @Override
    public boolean compact() {
        resetSpaceParams();

        int tupleCount = buf.getInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET);

        // determine start of target free space (depends on assumptions stated above)
        int freeSpace = buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET);
        int prefixTupleCount = buf.getInt(PREFIX_TUPLE_COUNT_OFFSET);
        if (prefixTupleCount > 0) {

            // debug
            int max = 0;
            for (int i = 0; i < prefixTupleCount; i++) {
                framePrefixTuple.resetByTupleIndex(this, i);
                int end = framePrefixTuple.getFieldStart(framePrefixTuple.getFieldCount() - 1)
                        + framePrefixTuple.getFieldLength(framePrefixTuple.getFieldCount() - 1);
                if (end > max) {
                    max = end;
                }
            }

            framePrefixTuple.resetByTupleIndex(this, prefixTupleCount - 1);
            freeSpace = framePrefixTuple.getFieldStart(framePrefixTuple.getFieldCount() - 1)
                    + framePrefixTuple.getFieldLength(framePrefixTuple.getFieldCount() - 1);
        }

        ArrayList<SlotOffTupleOff> sortedTupleOffs = new ArrayList<>();
        sortedTupleOffs.ensureCapacity(tupleCount);
        for (int i = 0; i < tupleCount; i++) {
            int tupleSlotOff = slotManager.getTupleSlotOff(i);
            int tupleSlot = buf.getInt(tupleSlotOff);
            int tupleOff = slotManager.decodeSecondSlotField(tupleSlot);
            sortedTupleOffs.add(new SlotOffTupleOff(i, tupleSlotOff, tupleOff));

        }
        Collections.sort(sortedTupleOffs);

        for (int i = 0; i < sortedTupleOffs.size(); i++) {
            int tupleOff = sortedTupleOffs.get(i).tupleOff;
            int tupleSlot = buf.getInt(sortedTupleOffs.get(i).slotOff);
            int prefixSlotNum = slotManager.decodeFirstSlotField(tupleSlot);

            frameTuple.resetByTupleIndex(this, sortedTupleOffs.get(i).tupleIndex);
            int tupleEndOff = frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                    + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1);
            int tupleLength = tupleEndOff - tupleOff;
            System.arraycopy(buf.array(), tupleOff, buf.array(), freeSpace, tupleLength);

            slotManager.setSlot(sortedTupleOffs.get(i).slotOff, slotManager.encodeSlotFields(prefixSlotNum, freeSpace));
            freeSpace += tupleLength;
        }

        buf.putInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET, freeSpace);
        int totalFreeSpace = buf.capacity() - buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET)
                - ((buf.getInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET) + buf.getInt(PREFIX_TUPLE_COUNT_OFFSET))
                        * slotManager.getSlotSize());
        buf.putInt(TOTAL_FREE_SPACE_OFFSET, totalFreeSpace);

        return false;
    }

    @Override
    public void delete(ITupleReference tuple, int slot) {
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        int prefixSlotNum = slotManager.decodeFirstSlotField(slot);
        int tupleSlotOff = slotManager.getTupleSlotOff(tupleIndex);

        // perform deletion (we just do a memcpy to overwrite the slot)
        int slotEndOff = slotManager.getTupleSlotEndOff();
        int length = tupleSlotOff - slotEndOff;
        System.arraycopy(buf.array(), slotEndOff, buf.array(), slotEndOff + slotManager.getSlotSize(), length);

        // maintain space information, get size of tuple suffix (suffix could be entire tuple)
        int tupleSize = 0;
        int suffixFieldStart = 0;
        if (prefixSlotNum == FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            suffixFieldStart = 0;
            buf.putInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET, buf.getInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET) - 1);
        } else {
            int prefixSlot = buf.getInt(slotManager.getPrefixSlotOff(prefixSlotNum));
            suffixFieldStart = slotManager.decodeFirstSlotField(prefixSlot);
        }

        frameTuple.resetByTupleIndex(this, tupleIndex);
        tupleSize =
                tupleWriter.bytesRequired(frameTuple, suffixFieldStart, frameTuple.getFieldCount() - suffixFieldStart);

        buf.putInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET,
                buf.getInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET) - 1);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) + tupleSize + slotManager.getSlotSize());
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) throws HyracksDataException {
        int freeContiguous = buf.capacity() - buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET)
                - ((buf.getInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET) + buf.getInt(PREFIX_TUPLE_COUNT_OFFSET))
                        * slotManager.getSlotSize());

        int bytesRequired = tupleWriter.bytesRequired(tuple);

        // See if the tuple would fit uncompressed.
        if (bytesRequired + slotManager.getSlotSize() <= freeContiguous) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }

        // See if tuple would fit into remaining space after compaction.
        if (bytesRequired + slotManager.getSlotSize() <= buf.getInt(TOTAL_FREE_SPACE_OFFSET)) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        }

        // See if the tuple matches a prefix and will fit after truncating the prefix.
        int prefixSlotNum = slotManager.findPrefix(tuple, framePrefixTuple);
        if (prefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);
            int prefixSlot = buf.getInt(prefixSlotOff);
            int numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);

            int compressedSize =
                    tupleWriter.bytesRequired(tuple, numPrefixFields, tuple.getFieldCount() - numPrefixFields);
            if (compressedSize + slotManager.getSlotSize() <= freeContiguous) {
                return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
            }
        }

        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        int slot = slotManager.insertSlot(tupleIndex, buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET));
        int prefixSlotNum = slotManager.decodeFirstSlotField(slot);
        int numPrefixFields = 0;
        if (prefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);
            int prefixSlot = buf.getInt(prefixSlotOff);
            numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
        } else {
            buf.putInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET, buf.getInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET) + 1);
        }

        int freeSpace = buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET);
        int bytesWritten = tupleWriter.writeTupleFields(tuple, numPrefixFields, tuple.getFieldCount() - numPrefixFields,
                buf.array(), freeSpace);

        buf.putInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET,
                buf.getInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET) + 1);
        buf.putInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET,
                buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET) + bytesWritten);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public FrameOpSpaceStatus hasSpaceUpdate(ITupleReference newTuple, int oldTupleIndex) {
        int tupleIndex = slotManager.decodeSecondSlotField(oldTupleIndex);
        frameTuple.resetByTupleIndex(this, tupleIndex);

        int oldTupleBytes = 0;
        int newTupleBytes = 0;

        int numPrefixFields = frameTuple.getNumPrefixFields();
        int fieldCount = frameTuple.getFieldCount();
        if (numPrefixFields != 0) {
            // Check the space requirements for updating the suffix of the original tuple.
            oldTupleBytes = frameTuple.getSuffixTupleSize();
            newTupleBytes = tupleWriter.bytesRequired(newTuple, numPrefixFields, fieldCount - numPrefixFields);
        } else {
            // The original tuple is uncompressed.
            oldTupleBytes = frameTuple.getTupleSize();
            newTupleBytes = tupleWriter.bytesRequired(newTuple);
        }

        int additionalBytesRequired = newTupleBytes - oldTupleBytes;
        // Enough space for an in-place update?
        if (additionalBytesRequired <= 0) {
            return FrameOpSpaceStatus.SUFFICIENT_INPLACE_SPACE;
        }

        int freeContiguous = buf.capacity() - buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET)
                - ((buf.getInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET) + buf.getInt(PREFIX_TUPLE_COUNT_OFFSET))
                        * slotManager.getSlotSize());

        // Enough space if we delete the old tuple and insert the new one without compaction?
        if (newTupleBytes <= freeContiguous) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }
        // Enough space if we delete the old tuple and compact?
        if (additionalBytesRequired <= buf.getInt(TOTAL_FREE_SPACE_OFFSET)) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        }
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    @Override
    public void update(ITupleReference newTuple, int oldTupleIndex, boolean inPlace) {
        int tupleIndex = slotManager.decodeSecondSlotField(oldTupleIndex);
        int tupleSlotOff = slotManager.getTupleSlotOff(tupleIndex);
        int tupleSlot = buf.getInt(tupleSlotOff);
        int prefixSlotNum = slotManager.decodeFirstSlotField(tupleSlot);
        int suffixTupleStartOff = slotManager.decodeSecondSlotField(tupleSlot);

        frameTuple.resetByTupleIndex(this, tupleIndex);
        int fieldCount = frameTuple.getFieldCount();
        int numPrefixFields = frameTuple.getNumPrefixFields();
        int oldTupleBytes = frameTuple.getSuffixTupleSize();
        int bytesWritten = 0;

        if (inPlace) {
            // Overwrite the old tuple suffix in place.
            bytesWritten = tupleWriter.writeTupleFields(newTuple, numPrefixFields, fieldCount - numPrefixFields,
                    buf.array(), suffixTupleStartOff);
        } else {
            // Insert the new tuple suffix at the end of the free space, and change
            // the slot value (effectively "deleting" the old tuple).
            int newSuffixTupleStartOff = buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET);
            bytesWritten = tupleWriter.writeTupleFields(newTuple, numPrefixFields, fieldCount - numPrefixFields,
                    buf.array(), newSuffixTupleStartOff);
            // Update slot value using the same prefix slot num.
            slotManager.setSlot(tupleSlotOff, slotManager.encodeSlotFields(prefixSlotNum, newSuffixTupleStartOff));
            // Update contiguous free space pointer.
            buf.putInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET, newSuffixTupleStartOff + bytesWritten);
        }
        buf.putInt(TOTAL_FREE_SPACE_OFFSET, buf.getInt(TOTAL_FREE_SPACE_OFFSET) + oldTupleBytes - bytesWritten);
    }

    protected void resetSpaceParams() {
        buf.putInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET, getOrigFreeSpaceOff());
        buf.putInt(TOTAL_FREE_SPACE_OFFSET, getOrigTotalFreeSpace());
    }

    @Override
    public void initBuffer(byte level) {
        buf.putLong(PAGE_LSN_OFFSET, 0);
        // during creation
        buf.putInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET, 0);
        resetSpaceParams();
        buf.putInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET, 0);
        buf.putInt(PREFIX_TUPLE_COUNT_OFFSET, 0);
        buf.put(ITreeIndexFrame.Constants.LEVEL_OFFSET, level);
        buf.put(SM_FLAG_OFFSET, (byte) 0);
        buf.putInt(NEXT_LEAF_OFFSET, -1);
    }

    public void setTotalFreeSpace(int totalFreeSpace) {
        buf.putInt(TOTAL_FREE_SPACE_OFFSET, totalFreeSpace);
    }

    public int getOrigTotalFreeSpace() {
        return buf.capacity() - (NEXT_LEAF_OFFSET + 4);
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        int slot;
        slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.EXCLUSIVE_ERROR_IF_EXISTS,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);

        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // Error indicator is set if there is an exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
        }
        return slot;
    }

    @Override
    public int findUpsertTupleIndex(ITupleReference tuple) throws HyracksDataException {
        int slot;
        slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.INCLUSIVE,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // Error indicator is set if there is an exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw HyracksDataException.create(ErrorCode.DUPLICATE_KEY);
        }
        return slot;
    }

    @Override
    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple, int targetTupleIndex)
            throws HyracksDataException {
        int tupleIndex = slotManager.decodeSecondSlotField(targetTupleIndex);
        // Examine the tuple index to determine whether it is valid or not.
        if (tupleIndex != slotManager.getGreatestKeyIndicator()) {
            // We need to check the key to determine whether it's an insert or an update.
            frameTuple.resetByTupleIndex(this, tupleIndex);
            if (cmp.compare(searchTuple, frameTuple) == 0) {
                // The keys match, it's an update.
                return frameTuple;
            }
        }
        // Either the tuple index is a special indicator, or the keys don't match.
        // In those cases, we are definitely dealing with an insert.
        return null;
    }

    @Override
    public int findUpdateTupleIndex(ITupleReference tuple) throws HyracksDataException {
        int slot;
        slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.EXACT,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw HyracksDataException.create(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY);
        }
        return slot;
    }

    @Override
    public int findDeleteTupleIndex(ITupleReference tuple) throws HyracksDataException {
        int slot;
        slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.EXACT,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw HyracksDataException.create(ErrorCode.UPDATE_OR_DELETE_NON_EXISTENT_KEY);
        }
        return slot;
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("pageLsnOff:                " + PAGE_LSN_OFFSET + "\n");
        strBuilder.append("tupleCountOff:             " + ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET + "\n");
        strBuilder.append("freeSpaceOff:              " + ITreeIndexFrame.Constants.FREE_SPACE_OFFSET + "\n");
        strBuilder.append("totalFreeSpaceOff:         " + TOTAL_FREE_SPACE_OFFSET + "\n");
        strBuilder.append("levelOff:                  " + ITreeIndexFrame.Constants.LEVEL_OFFSET + "\n");
        strBuilder.append("smFlagOff:                 " + SM_FLAG_OFFSET + "\n");
        strBuilder.append("uncompressedTupleCountOff: " + UNCOMPRESSED_TUPLE_COUNT_OFFSET + "\n");
        strBuilder.append("prefixTupleCountOff:       " + PREFIX_TUPLE_COUNT_OFFSET + "\n");
        strBuilder.append("nextLeafOff:               " + NEXT_LEAF_OFFSET + "\n");
        return strBuilder.toString();
    }

    @Override
    public int getTupleCount() {
        return buf.getInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET);
    }

    @Override
    public IPrefixSlotManager getSlotManager() {
        return slotManager;
    }

    @Override
    public int getTupleOffset(int slotNum) {
        int tupleSlotOff = slotManager.getTupleSlotOff(slotNum);
        int tupleSlot = buf.getInt(tupleSlotOff);
        return slotManager.decodeSecondSlotField(tupleSlot);
    }

    @Override
    public long getPageLsn() {
        return buf.getLong(PAGE_LSN_OFFSET);
    }

    @Override
    public void setPageLsn(long pageLsn) {
        buf.putLong(PAGE_LSN_OFFSET, pageLsn);
    }

    @Override
    public int getTotalFreeSpace() {
        return buf.getInt(TOTAL_FREE_SPACE_OFFSET);
    }

    @Override
    public boolean isLeaf() {
        return buf.get(ITreeIndexFrame.Constants.LEVEL_OFFSET) == 0;
    }

    @Override
    public boolean isInterior() {
        return buf.get(ITreeIndexFrame.Constants.LEVEL_OFFSET) > 0;
    }

    @Override
    public byte getLevel() {
        return buf.get(ITreeIndexFrame.Constants.LEVEL_OFFSET);
    }

    @Override
    public void setLevel(byte level) {
        buf.put(ITreeIndexFrame.Constants.LEVEL_OFFSET, level);
    }

    @Override
    public boolean getSmFlag() {
        return buf.get(SM_FLAG_OFFSET) != 0;
    }

    @Override
    public void setSmFlag(boolean smFlag) {
        if (smFlag) {
            buf.put(SM_FLAG_OFFSET, (byte) 1);
        } else {
            buf.put(SM_FLAG_OFFSET, (byte) 0);
        }
    }

    @Override
    public void setLargeFlag(boolean largePage) {
        throw new IllegalStateException();
    }

    @Override
    public boolean getLargeFlag() {
        return false;
    }

    public int getPrefixTupleCount() {
        return buf.getInt(PREFIX_TUPLE_COUNT_OFFSET);
    }

    public void setPrefixTupleCount(int prefixTupleCount) {
        buf.putInt(PREFIX_TUPLE_COUNT_OFFSET, prefixTupleCount);
    }

    @Override
    public void insertSorted(ITupleReference tuple) throws HyracksDataException {
        int freeSpace = buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET);
        int fieldsToTruncate = 0;

        // check if tuple matches last prefix tuple
        if (buf.getInt(PREFIX_TUPLE_COUNT_OFFSET) > 0) {
            framePrefixTuple.resetByTupleIndex(this, buf.getInt(PREFIX_TUPLE_COUNT_OFFSET) - 1);
            if (cmp.fieldRangeCompare(tuple, framePrefixTuple, 0, framePrefixTuple.getFieldCount()) == 0) {
                fieldsToTruncate = framePrefixTuple.getFieldCount();
            }
        }

        int bytesWritten = tupleWriter.writeTupleFields(tuple, fieldsToTruncate,
                tuple.getFieldCount() - fieldsToTruncate, buf.array(), freeSpace);

        // insert slot
        int prefixSlotNum = FieldPrefixSlotManager.TUPLE_UNCOMPRESSED;
        if (fieldsToTruncate > 0) {
            prefixSlotNum = buf.getInt(PREFIX_TUPLE_COUNT_OFFSET) - 1;
        } else {
            buf.putInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET, buf.getInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET) + 1);
        }
        int insSlot = slotManager.encodeSlotFields(prefixSlotNum, FieldPrefixSlotManager.GREATEST_KEY_INDICATOR);
        slotManager.insertSlot(insSlot, freeSpace);

        // update page metadata
        buf.putInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET,
                buf.getInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET) + 1);
        buf.putInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET,
                buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET) + bytesWritten);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey,
            IExtraPageBlockHelper extraPageBlockHelper, IBufferCache bufferCache) throws HyracksDataException {

        BTreeFieldPrefixNSMLeafFrame rf = (BTreeFieldPrefixNSMLeafFrame) rightFrame;

        ByteBuffer right = rf.getBuffer();
        int tupleCount = getTupleCount();
        int prefixTupleCount = getPrefixTupleCount();

        // Find split point, and determine into which frame the new tuple should
        // be inserted into.
        int tuplesToLeft;
        int midSlotNum = tupleCount / 2;
        ITreeIndexFrame targetFrame = null;
        frameTuple.resetByTupleIndex(this, midSlotNum);
        int comparison = cmp.compare(tuple, frameTuple);
        if (comparison >= 0) {
            tuplesToLeft = midSlotNum + (tupleCount % 2);
            targetFrame = rf;
        } else {
            tuplesToLeft = midSlotNum;
            targetFrame = this;
        }
        int tuplesToRight = tupleCount - tuplesToLeft;

        // copy entire page
        System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());

        // determine how many slots go on left and right page
        int prefixesToLeft = prefixTupleCount;
        for (int i = tuplesToLeft; i < tupleCount; i++) {
            int tupleSlotOff = rf.slotManager.getTupleSlotOff(i);
            int tupleSlot = right.getInt(tupleSlotOff);
            int prefixSlotNum = rf.slotManager.decodeFirstSlotField(tupleSlot);
            if (prefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
                prefixesToLeft = prefixSlotNum;
                break;
            }
        }

        // if we are splitting in the middle of a prefix both pages need to have the prefix slot and tuple
        int boundaryTupleSlotOff = rf.slotManager.getTupleSlotOff(tuplesToLeft - 1);
        int boundaryTupleSlot = buf.getInt(boundaryTupleSlotOff);
        int boundaryPrefixSlotNum = rf.slotManager.decodeFirstSlotField(boundaryTupleSlot);
        int prefixesToRight = prefixTupleCount - prefixesToLeft;
        if (boundaryPrefixSlotNum == prefixesToLeft
                && boundaryPrefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            prefixesToLeft++; // tuples on both pages share one prefix
        }

        // move prefix tuples on right page to beginning of page and adjust prefix slots
        if (prefixesToRight > 0 && prefixesToLeft > 0 && prefixTupleCount > 1) {

            int freeSpace = rf.getOrigFreeSpaceOff();
            int lastPrefixSlotNum = -1;

            for (int i = tuplesToLeft; i < tupleCount; i++) {
                int tupleSlotOff = rf.slotManager.getTupleSlotOff(i);
                int tupleSlot = right.getInt(tupleSlotOff);
                int prefixSlotNum = rf.slotManager.decodeFirstSlotField(tupleSlot);
                if (prefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
                    framePrefixTuple.resetByTupleIndex(this, prefixSlotNum);

                    int bytesWritten = 0;
                    if (lastPrefixSlotNum != prefixSlotNum) {
                        bytesWritten = tupleWriter.writeTuple(framePrefixTuple, right.array(), freeSpace);
                        int newPrefixSlot =
                                rf.slotManager.encodeSlotFields(framePrefixTuple.getFieldCount(), freeSpace);
                        int prefixSlotOff = rf.slotManager.getPrefixSlotOff(prefixSlotNum);
                        right.putInt(prefixSlotOff, newPrefixSlot);
                        lastPrefixSlotNum = prefixSlotNum;
                    }

                    int tupleOff = rf.slotManager.decodeSecondSlotField(tupleSlot);
                    int newTupleSlot = rf.slotManager
                            .encodeSlotFields(prefixSlotNum - (prefixTupleCount - prefixesToRight), tupleOff);
                    right.putInt(tupleSlotOff, newTupleSlot);
                    freeSpace += bytesWritten;
                }
            }
        }

        // move the modified prefix slots on the right page
        int prefixSrc = rf.slotManager.getPrefixSlotEndOff();
        int prefixDest = rf.slotManager.getPrefixSlotEndOff()
                + (prefixTupleCount - prefixesToRight) * rf.slotManager.getSlotSize();
        int prefixLength = rf.slotManager.getSlotSize() * prefixesToRight;
        System.arraycopy(right.array(), prefixSrc, right.array(), prefixDest, prefixLength);

        // on right page we need to copy rightmost tuple slots to left
        int src = rf.slotManager.getTupleSlotEndOff();
        int dest = rf.slotManager.getTupleSlotEndOff() + tuplesToLeft * rf.slotManager.getSlotSize()
                + (prefixTupleCount - prefixesToRight) * rf.slotManager.getSlotSize();
        int length = rf.slotManager.getSlotSize() * tuplesToRight;
        System.arraycopy(right.array(), src, right.array(), dest, length);

        right.putInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET, tuplesToRight);
        right.putInt(PREFIX_TUPLE_COUNT_OFFSET, prefixesToRight);

        // on left page move slots to reflect possibly removed prefixes
        src = slotManager.getTupleSlotEndOff() + tuplesToRight * slotManager.getSlotSize();
        dest = slotManager.getTupleSlotEndOff() + tuplesToRight * slotManager.getSlotSize()
                + (prefixTupleCount - prefixesToLeft) * slotManager.getSlotSize();
        length = slotManager.getSlotSize() * tuplesToLeft;
        System.arraycopy(buf.array(), src, buf.array(), dest, length);

        buf.putInt(ITreeIndexFrame.Constants.TUPLE_COUNT_OFFSET, tuplesToLeft);
        buf.putInt(PREFIX_TUPLE_COUNT_OFFSET, prefixesToLeft);

        // compact both pages
        compact();
        rightFrame.compact();

        // insert last key
        int targetTupleIndex;
        // it's safe to catch this exception since it will have been caught before reaching here
        targetTupleIndex = ((IBTreeLeafFrame) targetFrame).findInsertTupleIndex(tuple);
        targetFrame.insert(tuple, targetTupleIndex);

        // set split key to be highest value in left page
        frameTuple.resetByTupleIndex(this, getTupleCount() - 1);

        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(frameTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer().array(), 0);
    }

    @Override
    public int getFreeSpaceOff() {
        return buf.getInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET);
    }

    public int getOrigFreeSpaceOff() {
        return NEXT_LEAF_OFFSET + 4;
    }

    @Override
    public void setFreeSpaceOff(int freeSpace) {
        buf.putInt(ITreeIndexFrame.Constants.FREE_SPACE_OFFSET, freeSpace);
    }

    @Override
    public void setNextLeaf(int page) {
        buf.putInt(NEXT_LEAF_OFFSET, page);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(NEXT_LEAF_OFFSET);
    }

    public int getUncompressedTupleCount() {
        return buf.getInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET);
    }

    public void setUncompressedTupleCount(int uncompressedTupleCount) {
        buf.putInt(UNCOMPRESSED_TUPLE_COUNT_OFFSET, uncompressedTupleCount);
    }

    @Override
    public int getSlotSize() {
        return slotManager.getSlotSize();
    }

    @Override
    public BTreeTypeAwareTupleWriter getTupleWriter() {
        return tupleWriter;
    }

    @Override
    public IBTreeIndexTupleReference createTupleReference() {
        return new BTreeFieldPrefixTupleReference(tupleWriter.createTupleReference());
    }

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference pageTuple, MultiComparator cmp,
            FindTupleMode ftm, FindTupleNoExactMatchPolicy ftp) throws HyracksDataException {
        int slot = slotManager.findSlot(searchKey, pageTuple, framePrefixTuple, cmp, ftm, ftp);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // TODO: Revisit this one. Maybe there is a cleaner way to solve this in the RangeSearchCursor.
        if (tupleIndex == FieldPrefixSlotManager.GREATEST_KEY_INDICATOR
                || tupleIndex == FieldPrefixSlotManager.ERROR_INDICATOR) {
            return -1;
        } else {
            return tupleIndex;
        }
    }

    @Override
    public int getPageHeaderSize() {
        return NEXT_LEAF_OFFSET;
    }

    @Override
    public void setMultiComparator(MultiComparator cmp) {
        this.cmp = cmp;
        this.slotManager.setMultiComparator(cmp);
    }

    @Override
    public void validate(PageValidationInfo pvi) {
        // Do nothing
    }

    @Override
    public void ensureCapacity(IBufferCache bufferCache, ITupleReference tuple, IExtraPageBlockHelper helper)
            throws HyracksDataException {
        throw new IllegalStateException("nyi");
    }

    @Override
    public ITupleReference getLeftmostTuple() throws HyracksDataException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ITupleReference getRightmostTuple() throws HyracksDataException {
        throw new UnsupportedOperationException("Not implemented");
    }

}
