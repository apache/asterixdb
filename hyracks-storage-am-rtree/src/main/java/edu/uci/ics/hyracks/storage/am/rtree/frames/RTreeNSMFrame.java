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

package edu.uci.ics.hyracks.storage.am.rtree.frames;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TupleEntryArrayList;
import edu.uci.ics.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public abstract class RTreeNSMFrame extends TreeIndexNSMFrame implements IRTreeFrame {
    protected static final int pageNsnOff = smFlagOff + 1;
    protected static final int rightPageOff = pageNsnOff + 4;

    protected ITreeIndexTupleReference[] tuples;
    protected ITreeIndexTupleReference cmpFrameTuple;
    protected TupleEntryArrayList tupleEntries1; // used for split and checking
                                                 // enlargement
    protected TupleEntryArrayList tupleEntries2; // used for split

    protected Rectangle[] rec;

    protected static final double splitFactor = 0.4;
    protected static final int nearMinimumOverlapFactor = 32;
    private static final double doubleEpsilon = computeDoubleEpsilon();
    private static final int numTuplesEntries = 100;

    public RTreeNSMFrame(ITreeIndexTupleWriter tupleWriter, int keyFieldCount) {
        super(tupleWriter, new UnorderedSlotManager());
        this.tuples = new ITreeIndexTupleReference[keyFieldCount];
        for (int i = 0; i < keyFieldCount; i++) {
            this.tuples[i] = tupleWriter.createTupleReference();
        }
        cmpFrameTuple = tupleWriter.createTupleReference();

        tupleEntries1 = new TupleEntryArrayList(numTuplesEntries, numTuplesEntries);
        tupleEntries2 = new TupleEntryArrayList(numTuplesEntries, numTuplesEntries);
        rec = new Rectangle[4];
        for (int i = 0; i < 4; i++) {
            rec[i] = new Rectangle(keyFieldCount / 2);
        }
    }

    private static double computeDoubleEpsilon() {
        double doubleEpsilon = 1.0;

        do {
            doubleEpsilon /= 2.0;
        } while (1.0 + (doubleEpsilon / 2.0) != 1.0);
        return doubleEpsilon;
    }

    public static double doubleEpsilon() {
        return doubleEpsilon;
    }

    @Override
    public void initBuffer(byte level) {
        super.initBuffer(level);
        buf.putInt(pageNsnOff, 0);
        buf.putInt(rightPageOff, -1);
    }

    public void setTupleCount(int tupleCount) {
        buf.putInt(tupleCountOff, tupleCount);
    }

    @Override
    public void setPageNsn(int pageNsn) {
        buf.putInt(pageNsnOff, pageNsn);
    }

    @Override
    public int getPageNsn() {
        return buf.getInt(pageNsnOff);
    }

    @Override
    protected void resetSpaceParams() {
        buf.putInt(freeSpaceOff, rightPageOff + 4);
        buf.putInt(totalFreeSpaceOff, buf.capacity() - (rightPageOff + 4));
    }

    @Override
    public int getRightPage() {
        return buf.getInt(rightPageOff);
    }

    @Override
    public void setRightPage(int rightPage) {
        buf.putInt(rightPageOff, rightPage);
    }

    protected ITreeIndexTupleReference[] getTuples() {
        return tuples;
    }

    // for debugging
    public ArrayList<Integer> getChildren(MultiComparator cmp) {
        ArrayList<Integer> ret = new ArrayList<Integer>();
        frameTuple.setFieldCount(cmp.getKeyFieldCount());
        int tupleCount = buf.getInt(tupleCountOff);
        for (int i = 0; i < tupleCount; i++) {
            int tupleOff = slotManager.getTupleOff(slotManager.getSlotOff(i));
            frameTuple.resetByTupleOffset(buf, tupleOff);

            int intVal = IntegerSerializerDeserializer.getInt(
                    buf.array(),
                    frameTuple.getFieldStart(frameTuple.getFieldCount() - 1)
                            + frameTuple.getFieldLength(frameTuple.getFieldCount() - 1));
            ret.add(intVal);
        }
        return ret;
    }

    public void generateDist(ITupleReference tuple, TupleEntryArrayList entries, Rectangle rec, int start, int end) {
        int j = 0;
        while (entries.get(j).getTupleIndex() == -1) {
            j++;
        }
        frameTuple.resetByTupleIndex(this, entries.get(j).getTupleIndex());
        rec.set(frameTuple);
        for (int i = start; i < end; ++i) {
            if (i != j) {
                if (entries.get(i).getTupleIndex() != -1) {
                    frameTuple.resetByTupleIndex(this, entries.get(i).getTupleIndex());
                    rec.enlarge(frameTuple);
                } else {
                    rec.enlarge(tuple);
                }
            }
        }
    }

    @Override
    public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {

    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    public void adjustMBRImpl(ITreeIndexTupleReference[] tuples, MultiComparator cmp) {
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 1; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            for (int j = 0; j < maxFieldPos; j++) {
                int k = maxFieldPos + j;
                int c = cmp.getComparators()[j].compare(frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                        frameTuple.getFieldLength(j), tuples[j].getFieldData(j), tuples[j].getFieldStart(j),
                        tuples[j].getFieldLength(j));
                if (c < 0) {
                    tuples[j].resetByTupleIndex(this, i);
                }
                c = cmp.getComparators()[k].compare(frameTuple.getFieldData(k), frameTuple.getFieldStart(k),
                        frameTuple.getFieldLength(k), tuples[k].getFieldData(k), tuples[k].getFieldStart(k),
                        tuples[k].getFieldLength(k));
                if (c > 0) {
                    tuples[k].resetByTupleIndex(this, i);
                }
            }
        }
    }

    @Override
    public void computeMBR(ISplitKey splitKey, MultiComparator cmp) {
        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        frameTuple.setFieldCount(cmp.getFieldCount());

        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());

        splitKey.initData(splitKeySize);
        this.adjustMBR(tuples, cmp);
        rTreeTupleWriterLeftFrame.writeTupleFields(tuples, 0, rTreeSplitKey.getLeftPageBuffer(), 0);
        rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer(), 0);
    }

    @Override
    public int getPageHeaderSize() {
        return rightPageOff;
    }
}
