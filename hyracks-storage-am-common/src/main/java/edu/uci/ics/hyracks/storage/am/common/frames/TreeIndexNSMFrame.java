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

package edu.uci.ics.hyracks.storage.am.common.frames;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISlotManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.SlotOffTupleOff;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public abstract class TreeIndexNSMFrame implements ITreeIndexFrame {

    protected static final int pageLsnOff = 0; // 0
    protected static final int tupleCountOff = pageLsnOff + 4; // 4
    protected static final int freeSpaceOff = tupleCountOff + 4; // 8
    protected static final int totalFreeSpaceOff = freeSpaceOff + 4; // 16
    protected static final byte levelOff = totalFreeSpaceOff + 4;
    protected static final byte smFlagOff = levelOff + 1;

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;
    protected ISlotManager slotManager;

    protected ITreeIndexTupleWriter tupleWriter;
    protected ITreeIndexTupleReference frameTuple;

    public TreeIndexNSMFrame(ITreeIndexTupleWriter tupleWriter, ISlotManager slotManager) {
        this.tupleWriter = tupleWriter;
        this.frameTuple = tupleWriter.createTupleReference();
        this.slotManager = slotManager;
        this.slotManager.setFrame(this);
    }

    @Override
    public void initBuffer(byte level) {
        buf.putInt(pageLsnOff, 0); // TODO: might to set to a different lsn
        // during creation
        buf.putInt(tupleCountOff, 0);
        resetSpaceParams();
        buf.put(levelOff, level);
        buf.put(smFlagOff, (byte) 0);
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
    public int getFreeSpaceOff() {
        return buf.getInt(freeSpaceOff);
    }

    @Override
    public void setFreeSpaceOff(int freeSpace) {
        buf.putInt(freeSpaceOff, freeSpace);
    }

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();
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
    public boolean compact(MultiComparator cmp) {
        resetSpaceParams();
        frameTuple.setFieldCount(cmp.getFieldCount());

        int tupleCount = buf.getInt(tupleCountOff);
        int freeSpace = buf.getInt(freeSpaceOff);

        ArrayList<SlotOffTupleOff> sortedTupleOffs = new ArrayList<SlotOffTupleOff>();
        sortedTupleOffs.ensureCapacity(tupleCount);
        for (int i = 0; i < tupleCount; i++) {
            int slotOff = slotManager.getSlotOff(i);
            int tupleOff = slotManager.getTupleOff(slotOff);
            sortedTupleOffs.add(new SlotOffTupleOff(i, slotOff, tupleOff));
        }
        Collections.sort(sortedTupleOffs);

        for (int i = 0; i < sortedTupleOffs.size(); i++) {
            int tupleOff = sortedTupleOffs.get(i).tupleOff;
            frameTuple.resetByTupleOffset(buf, tupleOff);

            int tupleEndOff = frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                    + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1);
            int tupleLength = tupleEndOff - tupleOff;
            System.arraycopy(buf.array(), tupleOff, buf.array(), freeSpace, tupleLength);

            slotManager.setSlot(sortedTupleOffs.get(i).slotOff, freeSpace);
            freeSpace += tupleLength;
        }

        buf.putInt(freeSpaceOff, freeSpace);
        buf.putInt(totalFreeSpaceOff, buf.capacity() - freeSpace - tupleCount * slotManager.getSlotSize());

        return false;
    }

    @Override
    public void delete(ITupleReference tuple, MultiComparator cmp, int tupleIndex) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        // TODO: Fix me.
        //int tupleIndex = slotManager.findTupleIndex(tuple, frameTuple, cmp, FindTupleMode.FTM_EXACT,
        //        FindTupleNoExactMatchPolicy.FTP_HIGHER_KEY);
        int slotOff = slotManager.getSlotOff(tupleIndex);
        //if (tupleIndex < 0) {
        //    throw new TreeIndexException("Key to be deleted does not exist.");
        //}
        int tupleOff = slotManager.getTupleOff(slotOff);
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int tupleSize = tupleWriter.bytesRequired(frameTuple);

        // perform deletion (we just do a memcpy to overwrite the slot)
        int slotStartOff = slotManager.getSlotEndOff();
        int length = slotOff - slotStartOff;
        System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);

        // maintain space information
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) - 1);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + tupleSize + slotManager.getSlotSize());
    }

    @Override
    public FrameOpSpaceStatus hasSpaceInsert(ITupleReference tuple, MultiComparator cmp) {
        int bytesRequired = tupleWriter.bytesRequired(tuple);
        // Enough space in the contiguous space region?
        if (bytesRequired + slotManager.getSlotSize() <= buf.capacity() - buf.getInt(freeSpaceOff) 
                - (buf.getInt(tupleCountOff) * slotManager.getSlotSize())) {
            return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
        }
        // Enough space after compaction?
        if (bytesRequired + slotManager.getSlotSize() <= buf.getInt(totalFreeSpaceOff)) {
            return FrameOpSpaceStatus.SUFFICIENT_SPACE;
        }
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    @Override
    public FrameOpSpaceStatus hasSpaceUpdate(ITupleReference newTuple, int oldTupleIndex, MultiComparator cmp) {
    	frameTuple.resetByTupleIndex(this, oldTupleIndex);
    	frameTuple.setFieldCount(cmp.getFieldCount());
    	int oldTupleBytes = frameTuple.getTupleSize();
    	int newTupleBytes = tupleWriter.bytesRequired(newTuple);
    	int additionalBytesRequired = newTupleBytes - oldTupleBytes;
    	// Enough space for an in-place update?
    	if (additionalBytesRequired <= 0) {
    		return FrameOpSpaceStatus.SUFFICIENT_INPLACE_SPACE;
    	}
    	// Enough space if we delete the old tuple and insert the new one without compaction? 
    	if (newTupleBytes <= buf.capacity() - buf.getInt(freeSpaceOff)
                - (buf.getInt(tupleCountOff) * slotManager.getSlotSize())) {
    		return FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE;
    	}
    	// Enough space if we delete the old tuple and compact?
    	if (additionalBytesRequired <= buf.getInt(totalFreeSpaceOff)) {
    		return FrameOpSpaceStatus.SUFFICIENT_SPACE;
    	}
        return FrameOpSpaceStatus.INSUFFICIENT_SPACE;
    }

    protected void resetSpaceParams() {
        buf.putInt(freeSpaceOff, smFlagOff + 1);
        buf.putInt(totalFreeSpaceOff, buf.capacity() - (smFlagOff + 1));
    }

    @Override
    public void insert(ITupleReference tuple, MultiComparator cmp, int tupleIndex) {
        slotManager.insertSlot(tupleIndex, buf.getInt(freeSpaceOff));
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), buf.getInt(freeSpaceOff));
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void update(ITupleReference newTuple, int oldTupleIndex, boolean inPlace) {
    	frameTuple.resetByTupleIndex(this, oldTupleIndex);
		int oldTupleBytes = frameTuple.getTupleSize();
		int slotOff = slotManager.getSlotOff(oldTupleIndex);
		int bytesWritten = 0;
    	if (inPlace) {    		
    		// Overwrite the old tuple in place.
    		bytesWritten = tupleWriter.writeTuple(newTuple, buf.array(), buf.getInt(slotOff));
    	} else {
    		// Insert the new tuple at the end of the free space, and change the slot value (effectively "deleting" the old tuple).
    		int newTupleOff = buf.getInt(freeSpaceOff);
    		bytesWritten = tupleWriter.writeTuple(newTuple, buf.array(), newTupleOff);
    		// Update slot value.
    		buf.putInt(slotOff, newTupleOff);
    		// Update contiguous free space pointer.
    		buf.putInt(freeSpaceOff, newTupleOff + bytesWritten);
    	}
    	buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + oldTupleBytes - bytesWritten);
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
        return slotManager;
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
        return slotManager.getTupleOff(slotManager.getSlotStartOff() - slotNum * slotManager.getSlotSize());
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
    public boolean compress(MultiComparator cmp) {
        return false;
    }

    @Override
    public int getSlotSize() {
        return slotManager.getSlotSize();
    }

    @Override
    public ITreeIndexTupleWriter getTupleWriter() {
        return tupleWriter;
    }
    
    @Override
    public ITreeIndexTupleReference createTupleReference() {
    	return tupleWriter.createTupleReference();
    }
}
