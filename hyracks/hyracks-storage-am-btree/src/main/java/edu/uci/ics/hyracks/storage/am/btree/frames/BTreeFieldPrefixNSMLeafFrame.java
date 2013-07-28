/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.btree.frames;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.compressors.FieldPrefixCompressor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext.PageValidationInfo;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixPrefixTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameCompressor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.common.exceptions.TreeIndexNonExistentKeyException;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.SlotOffTupleOff;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

/**
 * WARNING: only works when tupleWriter is an instance of TypeAwareTupleWriter
 */
public class BTreeFieldPrefixNSMLeafFrame implements IBTreeLeafFrame {

    protected static final int pageLsnOff = 0; // 0
    protected static final int tupleCountOff = pageLsnOff + 8; // 8
    protected static final int freeSpaceOff = tupleCountOff + 4; // 12
    protected static final int totalFreeSpaceOff = freeSpaceOff + 4; // 16
    protected static final int levelOff = totalFreeSpaceOff + 4; // 20
    protected static final int smFlagOff = levelOff + 1; // 21
    protected static final int uncompressedTupleCountOff = smFlagOff + 1; // 22
    protected static final int prefixTupleCountOff = uncompressedTupleCountOff + 4; // 26
    protected static final int nextLeafOff = prefixTupleCountOff + 4; // 30

    private final IPrefixSlotManager slotManager;
    private final ITreeIndexFrameCompressor compressor;
    private final FieldPrefixTupleReference frameTuple;
    private final FieldPrefixPrefixTupleReference framePrefixTuple;
    private final ITreeIndexTupleWriter tupleWriter;

    private MultiComparator cmp;

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;

    public BTreeFieldPrefixNSMLeafFrame(ITreeIndexTupleWriter tupleWriter) {
        this.tupleWriter = tupleWriter;
        this.frameTuple = new FieldPrefixTupleReference(tupleWriter.createTupleReference());
        this.slotManager = new FieldPrefixSlotManager();

        ITypeTraits[] typeTraits = ((TypeAwareTupleWriter) tupleWriter).getTypeTraits();
        this.framePrefixTuple = new FieldPrefixPrefixTupleReference(typeTraits);
        this.compressor = new FieldPrefixCompressor(typeTraits, 0.001f, 2);
    }

    @Override
    public int getMaxTupleSize(int pageSize) {
        return (pageSize - getPageHeaderSize()) / 2;
    }

    @Override
    public int getBytesRequriedToWriteTuple(ITupleReference tuple) {
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
            throw new HyracksDataException(e);
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

        int tupleCount = buf.getInt(tupleCountOff);

        // determine start of target free space (depends on assumptions stated above)
        int freeSpace = buf.getInt(freeSpaceOff);
        int prefixTupleCount = buf.getInt(prefixTupleCountOff);
        if (prefixTupleCount > 0) {

            // debug
            int max = 0;
            for (int i = 0; i < prefixTupleCount; i++) {
                framePrefixTuple.resetByTupleIndex(this, i);
                int end = framePrefixTuple.getFieldStart(framePrefixTuple.getFieldCount() - 1)
                        + framePrefixTuple.getFieldLength(framePrefixTuple.getFieldCount() - 1);
                if (end > max)
                    max = end;
            }

            framePrefixTuple.resetByTupleIndex(this, prefixTupleCount - 1);
            freeSpace = framePrefixTuple.getFieldStart(framePrefixTuple.getFieldCount() - 1)
                    + framePrefixTuple.getFieldLength(framePrefixTuple.getFieldCount() - 1);
        }

        ArrayList<SlotOffTupleOff> sortedTupleOffs = new ArrayList<SlotOffTupleOff>();
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

        buf.putInt(freeSpaceOff, freeSpace);
        int totalFreeSpace = buf.capacity() - buf.getInt(freeSpaceOff)
                - ((buf.getInt(tupleCountOff) + buf.getInt(prefixTupleCountOff)) * slotManager.getSlotSize());
        buf.putInt(totalFreeSpaceOff, totalFreeSpace);

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
            buf.putInt(uncompressedTupleCountOff, buf.getInt(uncompressedTupleCountOff) - 1);
        } else {
            int prefixSlot = buf.getInt(slotManager.getPrefixSlotOff(prefixSlotNum));
            suffixFieldStart = slotManager.decodeFirstSlotField(prefixSlot);
        }

        frameTuple.resetByTupleIndex(this, tupleIndex);
        tupleSize = tupleWriter.bytesRequired(frameTuple, suffixFieldStart, frameTuple.getFieldCount()
                - suffixFieldStart);

        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) - 1);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + tupleSize + slotManager.getSlotSize());
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple) {
        int freeContiguous = buf.capacity() - buf.getInt(freeSpaceOff)
                - ((buf.getInt(tupleCountOff) + buf.getInt(prefixTupleCountOff)) * slotManager.getSlotSize());

        int bytesRequired = tupleWriter.bytesRequired(tuple);

        // See if the tuple would fit uncompressed.
        if (bytesRequired + slotManager.getSlotSize() <= freeContiguous)
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;

        // See if tuple would fit into remaining space after compaction.
        if (bytesRequired + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff))
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;

        // See if the tuple matches a prefix and will fit after truncating the prefix.
        int prefixSlotNum = slotManager.findPrefix(tuple, framePrefixTuple);
        if (prefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);
            int prefixSlot = buf.getInt(prefixSlotOff);
            int numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);

            int compressedSize = tupleWriter.bytesRequired(tuple, numPrefixFields, tuple.getFieldCount()
                    - numPrefixFields);
            if (compressedSize + slotManager.getSlotSize() <= freeContiguous)
                return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }

        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        int slot = slotManager.insertSlot(tupleIndex, buf.getInt(freeSpaceOff));
        int prefixSlotNum = slotManager.decodeFirstSlotField(slot);
        int numPrefixFields = 0;
        if (prefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            int prefixSlotOff = slotManager.getPrefixSlotOff(prefixSlotNum);
            int prefixSlot = buf.getInt(prefixSlotOff);
            numPrefixFields = slotManager.decodeFirstSlotField(prefixSlot);
        } else {
            buf.putInt(uncompressedTupleCountOff, buf.getInt(uncompressedTupleCountOff) + 1);
        }

        int freeSpace = buf.getInt(freeSpaceOff);
        int bytesWritten = tupleWriter.writeTupleFields(tuple, numPrefixFields,
                tuple.getFieldCount() - numPrefixFields, buf.array(), freeSpace);

        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
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

        int freeContiguous = buf.capacity() - buf.getInt(freeSpaceOff)
                - ((buf.getInt(tupleCountOff) + buf.getInt(prefixTupleCountOff)) * slotManager.getSlotSize());

        // Enough space if we delete the old tuple and insert the new one without compaction?
        if (newTupleBytes <= freeContiguous) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }
        // Enough space if we delete the old tuple and compact?
        if (additionalBytesRequired <= buf.getInt(totalFreeSpaceOff)) {
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
            int newSuffixTupleStartOff = buf.getInt(freeSpaceOff);
            bytesWritten = tupleWriter.writeTupleFields(newTuple, numPrefixFields, fieldCount - numPrefixFields,
                    buf.array(), newSuffixTupleStartOff);
            // Update slot value using the same prefix slot num.
            slotManager.setSlot(tupleSlotOff, slotManager.encodeSlotFields(prefixSlotNum, newSuffixTupleStartOff));
            // Update contiguous free space pointer.
            buf.putInt(freeSpaceOff, newSuffixTupleStartOff + bytesWritten);
        }
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + oldTupleBytes - bytesWritten);
    }

    protected void resetSpaceParams() {
        buf.putInt(freeSpaceOff, getOrigFreeSpaceOff());
        buf.putInt(totalFreeSpaceOff, getOrigTotalFreeSpace());
    }

    @Override
    public void initBuffer(byte level) {
        buf.putLong(pageLsnOff, 0);
        // during creation
        buf.putInt(tupleCountOff, 0);
        resetSpaceParams();
        buf.putInt(uncompressedTupleCountOff, 0);
        buf.putInt(prefixTupleCountOff, 0);
        buf.put(levelOff, level);
        buf.put(smFlagOff, (byte) 0);
        buf.putInt(nextLeafOff, -1);
    }

    public void setTotalFreeSpace(int totalFreeSpace) {
        buf.putInt(totalFreeSpaceOff, totalFreeSpace);
    }

    public int getOrigTotalFreeSpace() {
        return buf.capacity() - (nextLeafOff + 4);
    }

    @Override
    public int findInsertTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp,
                FindTupleMode.EXCLUSIVE_ERROR_IF_EXISTS, FindTupleNoExactMatchPolicy.HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // Error indicator is set if there is an exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw new TreeIndexDuplicateKeyException("Trying to insert duplicate key into leaf node.");
        }
        return slot;
    }

    @Override
    public int findUpsertTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.INCLUSIVE,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // Error indicator is set if there is an exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw new TreeIndexDuplicateKeyException("Trying to insert duplicate key into leaf node.");
        }
        return slot;
    }

    @Override
    public ITupleReference getMatchingKeyTuple(ITupleReference searchTuple, int targetTupleIndex) {
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
    public int findUpdateTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.EXACT,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw new TreeIndexNonExistentKeyException("Trying to update a tuple with a nonexistent key in leaf node.");
        }
        return slot;
    }

    @Override
    public int findDeleteTupleIndex(ITupleReference tuple) throws TreeIndexException {
        int slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.EXACT,
                FindTupleNoExactMatchPolicy.HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // Error indicator is set if there is no exact match.
        if (tupleIndex == slotManager.getErrorIndicator()) {
            throw new TreeIndexNonExistentKeyException("Trying to delete a tuple with a nonexistent key in leaf node.");
        }
        return slot;
    }

    @Override
    public String printHeader() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("pageLsnOff:                " + pageLsnOff + "\n");
        strBuilder.append("tupleCountOff:             " + tupleCountOff + "\n");
        strBuilder.append("freeSpaceOff:              " + freeSpaceOff + "\n");
        strBuilder.append("totalFreeSpaceOff:         " + totalFreeSpaceOff + "\n");
        strBuilder.append("levelOff:                  " + levelOff + "\n");
        strBuilder.append("smFlagOff:                 " + smFlagOff + "\n");
        strBuilder.append("uncompressedTupleCountOff: " + uncompressedTupleCountOff + "\n");
        strBuilder.append("prefixTupleCountOff:       " + prefixTupleCountOff + "\n");
        strBuilder.append("nextLeafOff:               " + nextLeafOff + "\n");
        return strBuilder.toString();
    }

    @Override
    public int getTupleCount() {
        return buf.getInt(tupleCountOff);
    }

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
        return buf.getLong(pageLsnOff);
    }

    @Override
    public void setPageLsn(long pageLsn) {
        buf.putLong(pageLsnOff, pageLsn);
    }

    @Override
    public int getTotalFreeSpace() {
        return buf.getInt(totalFreeSpaceOff);
    }

    @Override
    public boolean isLeaf() {
        return buf.get(levelOff) == 0;
    }

    @Override
    public boolean isInterior() {
        return buf.get(levelOff) > 0;
    }

    @Override
    public byte getLevel() {
        return buf.get(levelOff);
    }

    @Override
    public void setLevel(byte level) {
        buf.put(levelOff, level);
    }

    @Override
    public boolean getSmFlag() {
        return buf.get(smFlagOff) != 0;
    }

    @Override
    public void setSmFlag(boolean smFlag) {
        if (smFlag)
            buf.put(smFlagOff, (byte) 1);
        else
            buf.put(smFlagOff, (byte) 0);
    }

    public int getPrefixTupleCount() {
        return buf.getInt(prefixTupleCountOff);
    }

    public void setPrefixTupleCount(int prefixTupleCount) {
        buf.putInt(prefixTupleCountOff, prefixTupleCount);
    }

    @Override
    public void insertSorted(ITupleReference tuple) {
        int freeSpace = buf.getInt(freeSpaceOff);
        int fieldsToTruncate = 0;

        // check if tuple matches last prefix tuple
        if (buf.getInt(prefixTupleCountOff) > 0) {
            framePrefixTuple.resetByTupleIndex(this, buf.getInt(prefixTupleCountOff) - 1);
            if (cmp.fieldRangeCompare(tuple, framePrefixTuple, 0, framePrefixTuple.getFieldCount()) == 0) {
                fieldsToTruncate = framePrefixTuple.getFieldCount();
            }
        }

        int bytesWritten = tupleWriter.writeTupleFields(tuple, fieldsToTruncate, tuple.getFieldCount()
                - fieldsToTruncate, buf.array(), freeSpace);

        // insert slot
        int prefixSlotNum = FieldPrefixSlotManager.TUPLE_UNCOMPRESSED;
        if (fieldsToTruncate > 0)
            prefixSlotNum = buf.getInt(prefixTupleCountOff) - 1;
        else
            buf.putInt(uncompressedTupleCountOff, buf.getInt(uncompressedTupleCountOff) + 1);
        int insSlot = slotManager.encodeSlotFields(prefixSlotNum, FieldPrefixSlotManager.GREATEST_KEY_INDICATOR);
        slotManager.insertSlot(insSlot, freeSpace);

        // update page metadata
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey) {

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
                        int newPrefixSlot = rf.slotManager
                                .encodeSlotFields(framePrefixTuple.getFieldCount(), freeSpace);
                        int prefixSlotOff = rf.slotManager.getPrefixSlotOff(prefixSlotNum);
                        right.putInt(prefixSlotOff, newPrefixSlot);
                        lastPrefixSlotNum = prefixSlotNum;
                    }

                    int tupleOff = rf.slotManager.decodeSecondSlotField(tupleSlot);
                    int newTupleSlot = rf.slotManager.encodeSlotFields(prefixSlotNum
                            - (prefixTupleCount - prefixesToRight), tupleOff);
                    right.putInt(tupleSlotOff, newTupleSlot);
                    freeSpace += bytesWritten;
                }
            }
        }

        // move the modified prefix slots on the right page
        int prefixSrc = rf.slotManager.getPrefixSlotEndOff();
        int prefixDest = rf.slotManager.getPrefixSlotEndOff() + (prefixTupleCount - prefixesToRight)
                * rf.slotManager.getSlotSize();
        int prefixLength = rf.slotManager.getSlotSize() * prefixesToRight;
        System.arraycopy(right.array(), prefixSrc, right.array(), prefixDest, prefixLength);

        // on right page we need to copy rightmost tuple slots to left
        int src = rf.slotManager.getTupleSlotEndOff();
        int dest = rf.slotManager.getTupleSlotEndOff() + tuplesToLeft * rf.slotManager.getSlotSize()
                + (prefixTupleCount - prefixesToRight) * rf.slotManager.getSlotSize();
        int length = rf.slotManager.getSlotSize() * tuplesToRight;
        System.arraycopy(right.array(), src, right.array(), dest, length);

        right.putInt(tupleCountOff, tuplesToRight);
        right.putInt(prefixTupleCountOff, prefixesToRight);

        // on left page move slots to reflect possibly removed prefixes
        src = slotManager.getTupleSlotEndOff() + tuplesToRight * slotManager.getSlotSize();
        dest = slotManager.getTupleSlotEndOff() + tuplesToRight * slotManager.getSlotSize()
                + (prefixTupleCount - prefixesToLeft) * slotManager.getSlotSize();
        length = slotManager.getSlotSize() * tuplesToLeft;
        System.arraycopy(buf.array(), src, buf.array(), dest, length);

        buf.putInt(tupleCountOff, tuplesToLeft);
        buf.putInt(prefixTupleCountOff, prefixesToLeft);

        // compact both pages
        compact();
        rightFrame.compact();

        // insert last key
        int targetTupleIndex;
        // it's safe to catch this exception since it will have been caught before reaching here
        try {
            targetTupleIndex = ((IBTreeLeafFrame) targetFrame).findInsertTupleIndex(tuple);
        } catch (TreeIndexException e) {
            throw new IllegalStateException(e);
        }
        targetFrame.insert(tuple, targetTupleIndex);

        // set split key to be highest value in left page
        frameTuple.resetByTupleIndex(this, getTupleCount() - 1);

        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(frameTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer().array(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);
    }

    @Override
    public int getFreeSpaceOff() {
        return buf.getInt(freeSpaceOff);
    }

    public int getOrigFreeSpaceOff() {
        return nextLeafOff + 4;
    }

    @Override
    public void setFreeSpaceOff(int freeSpace) {
        buf.putInt(freeSpaceOff, freeSpace);
    }

    @Override
    public void setNextLeaf(int page) {
        buf.putInt(nextLeafOff, page);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(nextLeafOff);
    }

    public int getUncompressedTupleCount() {
        return buf.getInt(uncompressedTupleCountOff);
    }

    public void setUncompressedTupleCount(int uncompressedTupleCount) {
        buf.putInt(uncompressedTupleCountOff, uncompressedTupleCount);
    }

    @Override
    public int getSlotSize() {
        return slotManager.getSlotSize();
    }

    public ITreeIndexTupleWriter getTupleWriter() {
        return tupleWriter;
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return new FieldPrefixTupleReference(tupleWriter.createTupleReference());
    }

    @Override
    public int findTupleIndex(ITupleReference searchKey, ITreeIndexTupleReference pageTuple, MultiComparator cmp,
            FindTupleMode ftm, FindTupleNoExactMatchPolicy ftp) {
        int slot = slotManager.findSlot(searchKey, pageTuple, framePrefixTuple, cmp, ftm, ftp);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        // TODO: Revisit this one. Maybe there is a cleaner way to solve this in the RangeSearchCursor.
        if (tupleIndex == FieldPrefixSlotManager.GREATEST_KEY_INDICATOR
                || tupleIndex == FieldPrefixSlotManager.ERROR_INDICATOR)
            return -1;
        else
            return tupleIndex;
    }

    @Override
    public int getPageHeaderSize() {
        return nextLeafOff;
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
}
