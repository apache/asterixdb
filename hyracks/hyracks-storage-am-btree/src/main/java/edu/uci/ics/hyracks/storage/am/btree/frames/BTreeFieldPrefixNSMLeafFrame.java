/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IFrameCompressor;
import edu.uci.ics.hyracks.storage.am.btree.api.IPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.compressors.FieldPrefixCompressor;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeDuplicateKeyException;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixPrefixTupleReference;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.impls.FieldPrefixTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISlotManager;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleMode;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.FindTupleNoExactMatchPolicy;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.SlotOffTupleOff;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

// WARNING: only works when tupleWriter is an instance of TypeAwareTupleWriter

public class BTreeFieldPrefixNSMLeafFrame implements IBTreeLeafFrame {

    protected static final int pageLsnOff = 0; // 0
    protected static final int tupleCountOff = pageLsnOff + 4; // 4
    protected static final int freeSpaceOff = tupleCountOff + 4; // 8
    protected static final int totalFreeSpaceOff = freeSpaceOff + 4; // 12
    protected static final int levelOff = totalFreeSpaceOff + 4; // 16
    protected static final int smFlagOff = levelOff + 1; // 17
    protected static final int uncompressedTupleCountOff = smFlagOff + 1; // 18
    protected static final int prefixTupleCountOff = uncompressedTupleCountOff + 4; // 21

    protected static final int prevLeafOff = prefixTupleCountOff + 4; // 22
    protected static final int nextLeafOff = prevLeafOff + 4; // 26

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;
    public IFrameCompressor compressor;
    public IPrefixSlotManager slotManager; // TODO: should be protected, but
    // will trigger some refactoring

    private ITreeIndexTupleWriter tupleWriter;

    private FieldPrefixTupleReference frameTuple;
    private FieldPrefixPrefixTupleReference framePrefixTuple;

    public BTreeFieldPrefixNSMLeafFrame(ITreeIndexTupleWriter tupleWriter) {
        this.tupleWriter = tupleWriter;
        this.frameTuple = new FieldPrefixTupleReference(tupleWriter.createTupleReference());
        ITypeTrait[] typeTraits = ((TypeAwareTupleWriter) tupleWriter).getTypeTraits();
        this.framePrefixTuple = new FieldPrefixPrefixTupleReference(typeTraits);
        this.slotManager = new FieldPrefixSlotManager();
        this.compressor = new FieldPrefixCompressor(typeTraits, 0.001f, 2);
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
    public boolean compress(MultiComparator cmp) throws HyracksDataException {
        try {
            return compressor.compress(this, cmp);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    // assumptions:
    // 1. prefix tuple are stored contiguously
    // 2. prefix tuple are located before tuples (physically on the page)
    // 3. prefix tuple are sorted (last prefix tuple is at highest offset)
    // this procedure will not move prefix tuples
    @Override
    public boolean compact(MultiComparator cmp) {
        resetSpaceParams();

        frameTuple.setFieldCount(cmp.getFieldCount());

        int tupleCount = buf.getInt(tupleCountOff);

        // determine start of target free space (depends on assumptions stated
        // above)
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
    public void delete(ITupleReference tuple, MultiComparator cmp, boolean exactDelete) throws Exception {
        int slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.FTM_EXACT,
                FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        if (tupleIndex == FieldPrefixSlotManager.GREATEST_SLOT) {
            throw new BTreeException("Key to be deleted does not exist.");
        } else {
            int prefixSlotNum = slotManager.decodeFirstSlotField(slot);
            int tupleSlotOff = slotManager.getTupleSlotOff(tupleIndex);

            if (exactDelete) {
                frameTuple.setFieldCount(cmp.getFieldCount());
                frameTuple.resetByTupleIndex(this, tupleIndex);

                int comparison = cmp.fieldRangeCompare(tuple, frameTuple, cmp.getKeyFieldCount() - 1,
                        cmp.getFieldCount() - cmp.getKeyFieldCount());
                if (comparison != 0) {
                    throw new BTreeException("Cannot delete tuple. Byte-by-byte comparison failed to prove equality.");
                }
            }

            // perform deletion (we just do a memcpy to overwrite the slot)
            int slotEndOff = slotManager.getTupleSlotEndOff();
            int length = tupleSlotOff - slotEndOff;
            System.arraycopy(buf.array(), slotEndOff, buf.array(), slotEndOff + slotManager.getSlotSize(), length);

            // maintain space information, get size of tuple suffix (suffix
            // could be entire tuple)
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
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple, MultiComparator cmp) {
        int freeContiguous = buf.capacity() - buf.getInt(freeSpaceOff)
                - ((buf.getInt(tupleCountOff) + buf.getInt(prefixTupleCountOff)) * slotManager.getSlotSize());

        int bytesRequired = tupleWriter.bytesRequired(tuple);

        // see if the tuple would fit uncompressed
        if (bytesRequired + slotManager.getSlotSize() <= freeContiguous)
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;

        // see if tuple would fit into remaining space after compaction
        if (bytesRequired + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff))
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;

        // see if the tuple matches a prefix and will fit after truncating the
        // prefix
        int prefixSlotNum = slotManager.findPrefix(tuple, framePrefixTuple, cmp);
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
    public FrameOpSpaceStatus hasSpaceUpdate(int rid, ITupleReference tuple, MultiComparator cmp) {
        // TODO Auto-generated method stub
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    protected void resetSpaceParams() {
        buf.putInt(freeSpaceOff, getOrigFreeSpaceOff());
        buf.putInt(totalFreeSpaceOff, getOrigTotalFreeSpace());
    }

    @Override
    public void initBuffer(byte level) {
        buf.putInt(pageLsnOff, 0); // TODO: might to set to a different lsn
        // during creation
        buf.putInt(tupleCountOff, 0);
        resetSpaceParams();
        buf.putInt(uncompressedTupleCountOff, 0);
        buf.putInt(prefixTupleCountOff, 0);
        buf.put(levelOff, level);
        buf.put(smFlagOff, (byte) 0);
        buf.putInt(prevLeafOff, -1);
        buf.putInt(nextLeafOff, -1);
    }

    public void setTotalFreeSpace(int totalFreeSpace) {
        buf.putInt(totalFreeSpaceOff, totalFreeSpace);
    }

    public int getOrigTotalFreeSpace() {
        return buf.capacity() - (nextLeafOff + 4);
    }

    @Override
    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp) throws Exception {
        int slot = slotManager.findSlot(tuple, frameTuple, framePrefixTuple, cmp, FindTupleMode.FTM_INCLUSIVE,
                FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY);
        int tupleIndex = slotManager.decodeSecondSlotField(slot);
        if (tupleIndex != FieldPrefixSlotManager.GREATEST_SLOT) {
            frameTuple.setFieldCount(cmp.getFieldCount());
            frameTuple.resetByTupleIndex(this, tupleIndex);
            int comparison = cmp.fieldRangeCompare(tuple, frameTuple, 0, cmp.getKeyFieldCount());
            if (comparison == 0) {
                throw new BTreeDuplicateKeyException("Trying to insert duplicate key into leaf node.");
            }
        }
        return slot;
    }

    @Override
    public void insert(ITupleReference tuple, MultiComparator cmp, int tupleIndex) throws Exception {
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
                tuple.getFieldCount() - numPrefixFields, buf, freeSpace);

        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void update(int rid, ITupleReference tuple) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void printHeader() {
        // TODO Auto-generated method stub

    }

    @Override
    public int getTupleCount() {
        return buf.getInt(tupleCountOff);
    }

    public ISlotManager getSlotManager() {
        return null;
    }

    @Override
    public String printKeys(MultiComparator cmp, ISerializerDeserializer[] fields) throws HyracksDataException {
        StringBuilder strBuilder = new StringBuilder();
        int tupleCount = buf.getInt(tupleCountOff);
        frameTuple.setFieldCount(fields.length);
        for (int i = 0; i < tupleCount; i++) {
            frameTuple.resetByTupleIndex(this, i);
            for (int j = 0; j < cmp.getKeyFieldCount(); j++) {
                ByteArrayInputStream inStream = new ByteArrayInputStream(frameTuple.getFieldData(j),
                        frameTuple.getFieldStart(j), frameTuple.getFieldLength(j));
                DataInput dataIn = new DataInputStream(inStream);
                Object o = fields[j].deserialize(dataIn);
                strBuilder.append(o.toString() + " ");
            }
            strBuilder.append(" | ");
        }
        strBuilder.append("\n");
        return strBuilder.toString();
    }

    @Override
    public int getTupleOffset(int slotNum) {
        int tupleSlotOff = slotManager.getTupleSlotOff(slotNum);
        int tupleSlot = buf.getInt(tupleSlotOff);
        return slotManager.decodeSecondSlotField(tupleSlot);
    }

    @Override
    public int getPageLsn() {
        return buf.getInt(pageLsnOff);
    }

    @Override
    public void setPageLsn(int pageLsn) {
        buf.putInt(pageLsnOff, pageLsn);
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
    public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
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
                - fieldsToTruncate, buf, freeSpace);

        // insert slot
        int prefixSlotNum = FieldPrefixSlotManager.TUPLE_UNCOMPRESSED;
        if (fieldsToTruncate > 0)
            prefixSlotNum = buf.getInt(prefixTupleCountOff) - 1;
        else
            buf.putInt(uncompressedTupleCountOff, buf.getInt(uncompressedTupleCountOff) + 1);
        int insSlot = slotManager.encodeSlotFields(prefixSlotNum, FieldPrefixSlotManager.GREATEST_SLOT);
        slotManager.insertSlot(insSlot, freeSpace);

        // update page metadata
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey)
            throws Exception {

        BTreeFieldPrefixNSMLeafFrame rf = (BTreeFieldPrefixNSMLeafFrame) rightFrame;

        frameTuple.setFieldCount(cmp.getFieldCount());

        ByteBuffer right = rf.getBuffer();
        int tupleCount = getTupleCount();
        int prefixTupleCount = getPrefixTupleCount();

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

        // if we are splitting in the middle of a prefix both pages need to have
        // the prefix slot and tuple
        int boundaryTupleSlotOff = rf.slotManager.getTupleSlotOff(tuplesToLeft - 1);
        int boundaryTupleSlot = buf.getInt(boundaryTupleSlotOff);
        int boundaryPrefixSlotNum = rf.slotManager.decodeFirstSlotField(boundaryTupleSlot);
        int prefixesToRight = prefixTupleCount - prefixesToLeft;
        if (boundaryPrefixSlotNum == prefixesToLeft
                && boundaryPrefixSlotNum != FieldPrefixSlotManager.TUPLE_UNCOMPRESSED) {
            prefixesToLeft++; // tuples on both pages share one prefix
        }

        // move prefix tuples on right page to beginning of page and adjust
        // prefix slots
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
        compact(cmp);
        rightFrame.compact(cmp);

        // insert last key
        int targetTupleIndex = targetFrame.findTupleIndex(tuple, cmp);
        targetFrame.insert(tuple, cmp, targetTupleIndex);

        // set split key to be highest value in left page
        frameTuple.resetByTupleIndex(this, getTupleCount() - 1);

        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        splitKey.initData(splitKeySize);
        tupleWriter.writeTupleFields(frameTuple, 0, cmp.getKeyFieldCount(), splitKey.getBuffer(), 0);
        splitKey.getTuple().resetByTupleOffset(splitKey.getBuffer(), 0);

        return 0;
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
    public void setPrevLeaf(int page) {
        buf.putInt(prevLeafOff, page);
    }

    @Override
    public int getNextLeaf() {
        return buf.getInt(nextLeafOff);
    }

    @Override
    public int getPrevLeaf() {
        return buf.getInt(prevLeafOff);
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

    @Override
    public void setPageTupleFieldCount(int fieldCount) {
        frameTuple.setFieldCount(fieldCount);
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
        if (tupleIndex == FieldPrefixSlotManager.GREATEST_SLOT)
            return -1;
        else
            return tupleIndex;
    }

    @Override
    public int getPageHeaderSize() {
        return nextLeafOff;
    }
}
