package edu.uci.ics.hyracks.storage.am.rtree.frames;

import java.util.ArrayList;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.EntriesOrder;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.SpatialUtils;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TraverseList;
import edu.uci.ics.hyracks.storage.am.rtree.impls.TupleEntryArrayList;
import edu.uci.ics.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public class NSMFrame extends TreeIndexNSMFrame implements IRTreeFrame {
    protected static final int pageNsnOff = smFlagOff + 1;
    protected static final int rightPageOff = pageNsnOff + 4;

    private ITreeIndexTupleReference[] tuples;
    private ITreeIndexTupleReference cmpFrameTuple;
    public final SpatialUtils spatialUtils;
    public TupleEntryArrayList tupleEntries1; // used for split and checking enlargement
    public TupleEntryArrayList tupleEntries2; // used for split
    public Rectangle[] rec;

    private static final double splitFactor = 0.4;
    private static final int nearMinimumOverlapFactor = 32;

    public NSMFrame(ITreeIndexTupleWriter tupleWriter, int keyFieldCount) {
        super(tupleWriter, new UnorderedSlotManager());
        this.tuples = new ITreeIndexTupleReference[keyFieldCount];
        for (int i = 0; i < keyFieldCount; i++) {
            this.tuples[i] = tupleWriter.createTupleReference();
        }
        cmpFrameTuple = tupleWriter.createTupleReference();
        spatialUtils = new SpatialUtils();
        // TODO: find a better way to know number of entries per node
        tupleEntries1 = new TupleEntryArrayList(100, 100, spatialUtils);
        tupleEntries2 = new TupleEntryArrayList(100, 100, spatialUtils);
        rec = new Rectangle[4];
        for (int i = 0; i < 4; i++) {
            rec[i] = new Rectangle(keyFieldCount / 2);
        }
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

    private ITreeIndexTupleReference[] getTuples() {
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

    @Override
    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey) throws Exception {

        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame = ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());
        rightFrame.setPageTupleFieldCount(cmp.getFieldCount());
        frameTuple.setFieldCount(cmp.getFieldCount());

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
        for (int i = startIndex; i < endIndex; i++) { // TODO: is there a better
                                                      // way
            // to split the entries?
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

    private void generateDist(ITupleReference tuple, TupleEntryArrayList entries, Rectangle rec, int start, int end) {
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
        try {
            insert(tuple, cmp, -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }
    
    @Override
    public int getBestChildPageId(MultiComparator cmp) {
        return buf.getInt(frameTuple.getFieldStart(cmp.getKeyFieldCount()));
    }

    @Override
    public boolean findBestChild(ITupleReference tuple, MultiComparator cmp) {
        cmpFrameTuple.setFieldCount(cmp.getFieldCount());
        frameTuple.setFieldCount(cmp.getFieldCount());

        int bestChild = 0;
        double minEnlargedArea = Double.MAX_VALUE;

        // the children pointers in the node point to leaves
        if (getLevel() == 1) {
            // find least overlap enlargement, use minimum enlarged area to
            // break tie, if tie still exists use minimum area to break it
            for (int i = 0; i < getTupleCount(); ++i) {
                frameTuple.resetByTupleIndex(this, i);
                double enlargedArea = enlargedArea(frameTuple, tuple, cmp);
                tupleEntries1.add(i, enlargedArea);
                if (enlargedArea < minEnlargedArea) {
                    minEnlargedArea = enlargedArea;
                    bestChild = i;
                }
            }
            if (minEnlargedArea < tupleEntries1.getDoubleEpsilon() || minEnlargedArea > tupleEntries1.getDoubleEpsilon()) {
                minEnlargedArea = Double.MAX_VALUE;
                int k;
                if (getTupleCount() > nearMinimumOverlapFactor) {
                    // sort the entries based on their area enlargement needed
                    // to include the object
                    tupleEntries1.sort(EntriesOrder.ASCENDING, getTupleCount());
                    k = nearMinimumOverlapFactor;
                } else {
                    k = getTupleCount();
                }

                double minOverlap = Double.MAX_VALUE;
                int id = 0;
                for (int i = 0; i < k; ++i) {
                    double difference = 0.0;
                    for (int j = 0; j < getTupleCount(); ++j) {
                        frameTuple.resetByTupleIndex(this, j);
                        cmpFrameTuple.resetByTupleIndex(this, tupleEntries1.get(i).getTupleIndex());

                        int c = cmp.getIntCmp().compare(frameTuple.getFieldData(cmp.getKeyFieldCount()),
                                frameTuple.getFieldStart(cmp.getKeyFieldCount()),
                                frameTuple.getFieldLength(cmp.getKeyFieldCount()),
                                cmpFrameTuple.getFieldData(cmp.getKeyFieldCount()),
                                cmpFrameTuple.getFieldStart(cmp.getKeyFieldCount()),
                                cmpFrameTuple.getFieldLength(cmp.getKeyFieldCount()));
                        if (c != 0) {
                            double intersection = overlappedArea(frameTuple, tuple, cmpFrameTuple, cmp);
                            if (intersection != 0.0) {
                                difference += intersection - overlappedArea(frameTuple, null, cmpFrameTuple, cmp);
                            }
                        } else {
                            id = j;
                        }
                    }

                    double enlargedArea = enlargedArea(cmpFrameTuple, tuple, cmp);
                    if (difference < minOverlap) {
                        minOverlap = difference;
                        minEnlargedArea = enlargedArea;
                        bestChild = id;
                    } else if (difference == minOverlap) {
                        if (enlargedArea < minEnlargedArea) {
                            minEnlargedArea = enlargedArea;
                            bestChild = id;
                        } else if (enlargedArea == minEnlargedArea) {
                            double area = area(cmpFrameTuple, cmp);
                            frameTuple.resetByTupleIndex(this, bestChild);
                            double minArea = area(frameTuple, cmp);
                            if (area < minArea) {
                                bestChild = id;
                            }
                        }
                    }
                }
            }
        } else { // find minimum enlarged area, use minimum area to break tie
            for (int i = 0; i < getTupleCount(); i++) {
                frameTuple.resetByTupleIndex(this, i);
                double enlargedArea = enlargedArea(frameTuple, tuple, cmp);
                if (enlargedArea < minEnlargedArea) {
                    minEnlargedArea = enlargedArea;
                    bestChild = i;
                } else if (enlargedArea == minEnlargedArea) {
                    double area = area(frameTuple, cmp);
                    frameTuple.resetByTupleIndex(this, bestChild);
                    double minArea = area(frameTuple, cmp);
                    if (area < minArea) {
                        bestChild = i;
                    }
                }
            }
        }
        tupleEntries1.clear();

        frameTuple.resetByTupleIndex(this, bestChild);
        if (minEnlargedArea > 0.0) {
            return true;
        } else {
            return false;
        }
    }

    private double area(ITupleReference tuple, MultiComparator cmp) {
        double area = 1.0;
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            area *= DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j))
                    - DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i));
        }
        return area;
    }

    private double overlappedArea(ITupleReference tuple1, ITupleReference tupleToBeInserted, ITupleReference tuple2,
            MultiComparator cmp) {
        double area = 1.0;
        double f1, f2;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            double pHigh1, pLow1;
            if (tupleToBeInserted != null) {
                int c = cmp.getComparators()[i].compare(tuple1.getFieldData(i), tuple1.getFieldStart(i),
                        tuple1.getFieldLength(i), tupleToBeInserted.getFieldData(i),
                        tupleToBeInserted.getFieldStart(i), tupleToBeInserted.getFieldLength(i));
                if (c < 0) {
                    pLow1 = DoubleSerializerDeserializer.getDouble(tuple1.getFieldData(i), tuple1.getFieldStart(i));
                } else {
                    pLow1 = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(i),
                            tupleToBeInserted.getFieldStart(i));
                }

                c = cmp.getComparators()[j].compare(tuple1.getFieldData(j), tuple1.getFieldStart(j),
                        tuple1.getFieldLength(j), tupleToBeInserted.getFieldData(j),
                        tupleToBeInserted.getFieldStart(j), tupleToBeInserted.getFieldLength(j));
                if (c > 0) {
                    pHigh1 = DoubleSerializerDeserializer.getDouble(tuple1.getFieldData(j), tuple1.getFieldStart(j));
                } else {
                    pHigh1 = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(j),
                            tupleToBeInserted.getFieldStart(j));
                }
            } else {
                pLow1 = DoubleSerializerDeserializer.getDouble(tuple1.getFieldData(i), tuple1.getFieldStart(i));
                pHigh1 = DoubleSerializerDeserializer.getDouble(tuple1.getFieldData(j), tuple1.getFieldStart(j));
            }

            double pLow2 = DoubleSerializerDeserializer.getDouble(tuple2.getFieldData(i), tuple2.getFieldStart(i));
            double pHigh2 = DoubleSerializerDeserializer.getDouble(tuple2.getFieldData(j), tuple2.getFieldStart(j));

            if (pLow1 > pHigh2 || pHigh1 < pLow2) {
                return 0.0;
            }

            f1 = Math.max(pLow1, pLow2);
            f2 = Math.min(pHigh1, pHigh2);
            area *= f2 - f1;
        }
        return area;
    }

    private double enlargedArea(ITupleReference tuple, ITupleReference tupleToBeInserted, MultiComparator cmp) {
        double areaBeforeEnlarge = area(tuple, cmp);
        double areaAfterEnlarge = 1.0;

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            double pHigh, pLow;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                    tupleToBeInserted.getFieldLength(i));
            if (c < 0) {
                pLow = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i));
            } else {
                pLow = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(i),
                        tupleToBeInserted.getFieldStart(i));
            }

            c = cmp.getComparators()[j].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                    tupleToBeInserted.getFieldLength(j));
            if (c > 0) {
                pHigh = DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j));
            } else {
                pHigh = DoubleSerializerDeserializer.getDouble(tupleToBeInserted.getFieldData(j),
                        tupleToBeInserted.getFieldStart(j));
            }
            areaAfterEnlarge *= pHigh - pLow;
        }
        return areaAfterEnlarge - areaBeforeEnlarge;
    }

    @Override
    public void enlarge(ITupleReference tuple, MultiComparator cmp) {
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(frameTuple.getFieldData(i), frameTuple.getFieldStart(i),
                    frameTuple.getFieldLength(i), tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i));
            if (c > 0) {
                System.arraycopy(tuple.getFieldData(i), tuple.getFieldStart(i), frameTuple.getFieldData(i),
                        frameTuple.getFieldStart(i), tuple.getFieldLength(i));
            }
            c = cmp.getComparators()[j].compare(frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j), tuple.getFieldData(j), tuple.getFieldStart(j),
                    tuple.getFieldLength(j));
            if (c < 0) {
                System.arraycopy(tuple.getFieldData(j), tuple.getFieldStart(j), frameTuple.getFieldData(j),
                        frameTuple.getFieldStart(j), tuple.getFieldLength(j));
            }
        }
    }

    @Override
    public void adjustKey(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        if (tupleIndex == -1) {
            tupleIndex = findTupleByPointer(tuple, cmp);
        }
        if (tupleIndex != -1) {
            tupleWriter.writeTuple(tuple, buf, getTupleOffset(tupleIndex));
        }
    }

    @Override
    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, null, null);
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
    public int findTupleByPointer(int pageId, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        for (int i = 0; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            int id = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(cmp.getKeyFieldCount()),
                    frameTuple.getFieldStart(cmp.getKeyFieldCount()));
            if (id == pageId) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int findTupleByPointer(ITupleReference tuple, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        for (int i = 0; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            int c = cmp.getIntCmp().compare(frameTuple.getFieldData(cmp.getKeyFieldCount()),
                    frameTuple.getFieldStart(cmp.getKeyFieldCount()),
                    frameTuple.getFieldLength(cmp.getKeyFieldCount()), tuple.getFieldData(cmp.getKeyFieldCount()),
                    tuple.getFieldStart(cmp.getKeyFieldCount()), tuple.getFieldLength(cmp.getKeyFieldCount()));
            if (c == 0) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public int findTupleByPointer(ITupleReference tuple, TraverseList traverseList, int parentIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        for (int i = 0; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            int c = cmp.getIntCmp().compare(frameTuple.getFieldData(cmp.getKeyFieldCount()),
                    frameTuple.getFieldStart(cmp.getKeyFieldCount()),
                    frameTuple.getFieldLength(cmp.getKeyFieldCount()), tuple.getFieldData(cmp.getKeyFieldCount()),
                    tuple.getFieldStart(cmp.getKeyFieldCount()), tuple.getFieldLength(cmp.getKeyFieldCount()));
            if (c == 0) {
                return i;
            } else {
                int pageId = IntegerSerializerDeserializer.getInt(frameTuple.getFieldData(cmp.getKeyFieldCount()),
                        frameTuple.getFieldStart(cmp.getKeyFieldCount()));
                traverseList.add(pageId, -1, parentIndex);
            }
        }
        return -1;
    }

    @Override
    public void adjustMBR(ITreeIndexTupleReference[] tuples, MultiComparator cmp) {
        for (int i = 0; i < tuples.length; i++) {
            tuples[i].setFieldCount(cmp.getKeyFieldCount());
            tuples[i].resetByTupleIndex(this, 0);
        }

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
    public int getChildPageIdIfIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j));
            if (c > 0) {
                return -1;
            }
            c = cmp.getComparators()[i].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));

            if (c < 0) {
                return -1;
            }
        }
        return buf.getInt(frameTuple.getFieldStart(cmp.getKeyFieldCount()));
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
    public Rectangle checkIntersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j));
            if (c > 0) {
                return null;
            }
            c = cmp.getComparators()[i].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));

            if (c < 0) {
                return null;
            }
        }
        Rectangle rec = new Rectangle(maxFieldPos);
        rec.set(frameTuple);
        return rec;
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
    public boolean recomputeMBR(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        frameTuple.resetByTupleIndex(this, tupleIndex);

        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(frameTuple.getFieldData(i), frameTuple.getFieldStart(i),
                    frameTuple.getFieldLength(i), tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i));
            if (c != 0) {
                return true;
            }
            c = cmp.getComparators()[j].compare(frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j), tuple.getFieldData(j), tuple.getFieldStart(j),
                    tuple.getFieldLength(j));

            if (c != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getPageHeaderSize() {
        return rightPageOff;
    }
}
