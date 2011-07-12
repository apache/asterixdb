package edu.uci.ics.hyracks.storage.am.rtree.frames;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.EntriesOrder;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import edu.uci.ics.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public class NSMLeafFrame extends NSMFrame implements IRTreeLeafFrame {

    public NSMLeafFrame(ITreeIndexTupleWriter tupleWriter, int keyFieldCount) {
        super(tupleWriter, keyFieldCount);
    }

    @Override
    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, null, null);
    }

    @Override
    public boolean intersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j));
            if (c > 0) {
                return false;
            }
            c = cmp.getComparators()[i].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));

            if (c < 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey)
            throws Exception {

        rightFrame.setPageTupleFieldCount(cmp.getFieldCount());
        frameTuple.setFieldCount(cmp.getFieldCount());

        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame = ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());

        // calculations are based on the R*-tree paper
        int m = (int) Math.floor((getTupleCount() + 1) * splitFactor);
        int splitDistribution = getTupleCount() - (2 * m) + 2;

        // to calculate the minimum margin in order to pick the split axis
        double minMargin = Double.MAX_VALUE;
        int splitAxis = 0, sortOrder = 0;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            for (int k = 0; k < getTupleCount(); ++k) {

                frameTuple.resetByTupleIndex(this, k);

                double LowerKey = DoubleSerializerDeserializer.getDouble(frameTuple.getFieldData(i),
                        frameTuple.getFieldStart(i));
                double UpperKey = DoubleSerializerDeserializer.getDouble(frameTuple.getFieldData(j),
                        frameTuple.getFieldStart(j));

                tupleEntries1.add(k, LowerKey);
                tupleEntries2.add(k, UpperKey);
            }
            double LowerKey = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i));
            double UpperKey = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j));

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
            double key = DoubleSerializerDeserializer.getDouble(frameTuple.getFieldData(splitAxis + sortOrder),
                    frameTuple.getFieldStart(splitAxis + sortOrder));
            tupleEntries1.add(i, key);
        }
        double key = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(splitAxis + sortOrder),
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
                rightFrame.insert(frameTuple, cmp, -1);
                ((UnorderedSlotManager) slotManager).modifySlot(
                        slotManager.getSlotOff(tupleEntries1.get(i).getTupleIndex()), -1);
                totalBytes += tupleWriter.bytesRequired(frameTuple);
                numOfDeletedTuples++;
            } else {
                rightFrame.insert(tuple, cmp, -1);
                tupleInserted = true;
            }
        }

        ((UnorderedSlotManager) slotManager).deleteEmptySlots();

        // maintain space information
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + totalBytes
                + (slotManager.getSlotSize() * numOfDeletedTuples));

        // compact both pages
        rightFrame.compact(cmp);
        compact(cmp);

        if (!tupleInserted) {
            insert(tuple, cmp, -1);
        }

        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());

        splitKey.initData(splitKeySize);
        this.adjustMBR(tuples, cmp);
        rTreeTupleWriterLeftFrame.writeTupleFields(tuples, 0, rTreeSplitKey.getLeftPageBuffer(), 0);
        rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer(), 0);

        ((IRTreeFrame) rightFrame).adjustMBR(((NSMFrame) rightFrame).getTuples(), cmp);
        rTreeTupleWriterRightFrame.writeTupleFields(((NSMFrame) rightFrame).getTuples(), 0,
                rTreeSplitKey.getRightPageBuffer(), 0);
        rTreeSplitKey.getRightTuple().resetByTupleOffset(rTreeSplitKey.getRightPageBuffer(), 0);

        tupleEntries1.clear();
        tupleEntries2.clear();
        return 0;
    }

    @Override
    public void insert(ITupleReference tuple, MultiComparator cmp, int tupleIndex) throws Exception {
        frameTuple.setFieldCount(cmp.getFieldCount());
        slotManager.insertSlot(-1, buf.getInt(freeSpaceOff));
        int bytesWritten = tupleWriter.writeTuple(tuple, buf, buf.getInt(freeSpaceOff));

        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void delete(int tupleIndex, MultiComparator cmp) throws Exception {
        frameTuple.setFieldCount(cmp.getFieldCount());
        int slotOff = slotManager.getSlotOff(tupleIndex);

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
    public void adjustMBR(ITreeIndexTupleReference[] tuples, MultiComparator cmp) {
        for (int i = 0; i < tuples.length; i++) {
            tuples[i].setFieldCount(cmp.getFieldCount());
            tuples[i].resetByTupleIndex(this, 0);
        }

        adjustMBRImpl(tuples, cmp);
    }
}
