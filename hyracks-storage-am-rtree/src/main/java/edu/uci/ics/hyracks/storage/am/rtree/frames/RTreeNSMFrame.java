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

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.EntriesOrder;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TupleEntryArrayList;
import edu.uci.ics.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public abstract class RTreeNSMFrame extends TreeIndexNSMFrame implements IRTreeFrame {
    protected static final int pageNsnOff = smFlagOff + 1;
    protected static final int rightPageOff = pageNsnOff + 8;

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
    protected final IPrimitiveValueProvider[] keyValueProviders;

    public RTreeNSMFrame(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders) {
        super(tupleWriter, new UnorderedSlotManager());
        this.tuples = new ITreeIndexTupleReference[keyValueProviders.length];
        for (int i = 0; i < keyValueProviders.length; i++) {
            this.tuples[i] = tupleWriter.createTupleReference();
        }
        cmpFrameTuple = tupleWriter.createTupleReference();

        tupleEntries1 = new TupleEntryArrayList(numTuplesEntries, numTuplesEntries);
        tupleEntries2 = new TupleEntryArrayList(numTuplesEntries, numTuplesEntries);
        rec = new Rectangle[4];
        for (int i = 0; i < 4; i++) {
            rec[i] = new Rectangle(keyValueProviders.length / 2);
        }
        this.keyValueProviders = keyValueProviders;
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
        buf.putLong(pageNsnOff, 0);
        buf.putInt(rightPageOff, -1);
    }

    public void setTupleCount(int tupleCount) {
        buf.putInt(tupleCountOff, tupleCount);
    }

    @Override
    public void setPageNsn(long pageNsn) {
        buf.putLong(pageNsnOff, pageNsn);
    }

    @Override
    public long getPageNsn() {
        return buf.getLong(pageNsnOff);
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

    @Override
    public void split(ITreeIndexFrame rightFrame, ITupleReference tuple, ISplitKey splitKey) throws TreeIndexException {
        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame = ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());

        // calculations are based on the R*-tree paper
        int m = (int) Math.floor((getTupleCount() + 1) * splitFactor);
        int splitDistribution = getTupleCount() - (2 * m) + 2;

        // to calculate the minimum margin in order to pick the split axis
        double minMargin = Double.MAX_VALUE;
        int splitAxis = 0, sortOrder = 0;

        int maxFieldPos = keyValueProviders.length / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            for (int k = 0; k < getTupleCount(); ++k) {

                frameTuple.resetByTupleIndex(this, k);
                double LowerKey = keyValueProviders[i]
                        .getValue(frameTuple.getFieldData(i), frameTuple.getFieldStart(i));
                double UpperKey = keyValueProviders[j]
                        .getValue(frameTuple.getFieldData(j), frameTuple.getFieldStart(j));

                tupleEntries1.add(k, LowerKey);
                tupleEntries2.add(k, UpperKey);
            }
            double LowerKey = keyValueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i));
            double UpperKey = keyValueProviders[j].getValue(tuple.getFieldData(j), tuple.getFieldStart(j));

            tupleEntries1.add(-1, LowerKey);
            tupleEntries2.add(-1, UpperKey);

            tupleEntries1.sort(EntriesOrder.ASCENDING, getTupleCount() + 1);
            tupleEntries2.sort(EntriesOrder.ASCENDING, getTupleCount() + 1);

            double lowerMargin = 0.0, upperMargin = 0.0;
            // generate distribution
            for (int k = 1; k <= splitDistribution; ++k) {
                int d = m - 1 + k;

                generateDist(tuple, tupleEntries1, rec[0], 0, d);
                generateDist(tuple, tupleEntries2, rec[1], 0, d);
                generateDist(tuple, tupleEntries1, rec[2], d, getTupleCount() + 1);
                generateDist(tuple, tupleEntries2, rec[3], d, getTupleCount() + 1);

                // calculate the margin of the distributions
                lowerMargin += rec[0].margin() + rec[2].margin();
                upperMargin += rec[1].margin() + rec[3].margin();
            }
            double margin = Math.min(lowerMargin, upperMargin);

            // store minimum margin as split axis
            if (margin < minMargin) {
                minMargin = margin;
                splitAxis = i;
                sortOrder = (lowerMargin < upperMargin) ? 0 : 2;
            }

            tupleEntries1.clear();
            tupleEntries2.clear();
        }

        for (int i = 0; i < getTupleCount(); ++i) {
            frameTuple.resetByTupleIndex(this, i);
            double key = keyValueProviders[splitAxis + sortOrder].getValue(
                    frameTuple.getFieldData(splitAxis + sortOrder), frameTuple.getFieldStart(splitAxis + sortOrder));
            tupleEntries1.add(i, key);
        }
        double key = keyValueProviders[splitAxis + sortOrder].getValue(tuple.getFieldData(splitAxis + sortOrder),
                tuple.getFieldStart(splitAxis + sortOrder));
        tupleEntries1.add(-1, key);
        tupleEntries1.sort(EntriesOrder.ASCENDING, getTupleCount() + 1);

        double minArea = Double.MAX_VALUE;
        double minOverlap = Double.MAX_VALUE;
        int splitPoint = 0;
        for (int i = 1; i <= splitDistribution; ++i) {
            int d = m - 1 + i;

            generateDist(tuple, tupleEntries1, rec[0], 0, d);
            generateDist(tuple, tupleEntries1, rec[2], d, getTupleCount() + 1);

            double overlap = rec[0].overlappedArea(rec[2]);
            if (overlap < minOverlap) {
                splitPoint = d;
                minOverlap = overlap;
                minArea = rec[0].area() + rec[2].area();
            } else if (overlap == minOverlap) {
                double area = rec[0].area() + rec[2].area();
                if (area < minArea) {
                    splitPoint = d;
                    minArea = area;
                }
            }
        }
        int startIndex, endIndex;
        if (splitPoint < (getTupleCount() + 1) / 2) {
            startIndex = 0;
            endIndex = splitPoint;
        } else {
            startIndex = splitPoint;
            endIndex = (getTupleCount() + 1);
        }
        boolean tupleInserted = false;
        int totalBytes = 0, numOfDeletedTuples = 0;
        for (int i = startIndex; i < endIndex; i++) {
            if (tupleEntries1.get(i).getTupleIndex() != -1) {
                frameTuple.resetByTupleIndex(this, tupleEntries1.get(i).getTupleIndex());
                rightFrame.insert(frameTuple, -1);
                ((UnorderedSlotManager) slotManager).modifySlot(
                        slotManager.getSlotOff(tupleEntries1.get(i).getTupleIndex()), -1);
                totalBytes += getTupleSize(frameTuple);
                numOfDeletedTuples++;
            } else {
                rightFrame.insert(tuple, -1);
                tupleInserted = true;
            }
        }

        ((UnorderedSlotManager) slotManager).deleteEmptySlots();

        // maintain space information
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + totalBytes
                + (slotManager.getSlotSize() * numOfDeletedTuples));

        // compact both pages
        rightFrame.compact();
        compact();

        if (!tupleInserted) {
            insert(tuple, -1);
        }

        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, keyValueProviders.length);

        splitKey.initData(splitKeySize);
        this.adjustMBR();
        rTreeTupleWriterLeftFrame.writeTupleFields(getTuples(), 0, rTreeSplitKey.getLeftPageBuffer(), 0);
        rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer(), 0);

        ((IRTreeFrame) rightFrame).adjustMBR();
        rTreeTupleWriterRightFrame.writeTupleFields(((RTreeNSMFrame) rightFrame).getTuples(), 0,
                rTreeSplitKey.getRightPageBuffer(), 0);
        rTreeSplitKey.getRightTuple().resetByTupleOffset(rTreeSplitKey.getRightPageBuffer(), 0);

        tupleEntries1.clear();
        tupleEntries2.clear();
    }

    abstract public int getTupleSize(ITupleReference tuple);

    public void generateDist(ITupleReference tuple, TupleEntryArrayList entries, Rectangle rec, int start, int end) {
        int j = 0;
        while (entries.get(j).getTupleIndex() == -1) {
            j++;
        }
        frameTuple.resetByTupleIndex(this, entries.get(j).getTupleIndex());
        rec.set(frameTuple, keyValueProviders);
        for (int i = start; i < end; ++i) {
            if (i != j) {
                if (entries.get(i).getTupleIndex() != -1) {
                    frameTuple.resetByTupleIndex(this, entries.get(i).getTupleIndex());
                    rec.enlarge(frameTuple, keyValueProviders);
                } else {
                    rec.enlarge(tuple, keyValueProviders);
                }
            }
        }
    }

    public void adjustMBRImpl(ITreeIndexTupleReference[] tuples) {
        int maxFieldPos = keyValueProviders.length / 2;
        for (int i = 1; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            for (int j = 0; j < maxFieldPos; j++) {
                int k = maxFieldPos + j;
                double valA = keyValueProviders[j].getValue(frameTuple.getFieldData(j), frameTuple.getFieldStart(j));
                double valB = keyValueProviders[j].getValue(tuples[j].getFieldData(j), tuples[j].getFieldStart(j));
                if (valA < valB) {
                    tuples[j].resetByTupleIndex(this, i);
                }
                valA = keyValueProviders[k].getValue(frameTuple.getFieldData(k), frameTuple.getFieldStart(k));
                valB = keyValueProviders[k].getValue(tuples[k].getFieldData(k), tuples[k].getFieldStart(k));
                if (valA > valB) {
                    tuples[k].resetByTupleIndex(this, i);
                }
            }
        }
    }

    @Override
    public void adjustMBR() {
        for (int i = 0; i < tuples.length; i++) {
            tuples[i].setFieldCount(getFieldCount());
            tuples[i].resetByTupleIndex(this, 0);
        }

        adjustMBRImpl(tuples);
    }

    public abstract int getFieldCount();

    @Override
    public int getPageHeaderSize() {
        return rightPageOff;
    }
}