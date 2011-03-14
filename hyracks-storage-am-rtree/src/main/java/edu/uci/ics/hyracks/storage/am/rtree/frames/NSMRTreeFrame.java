package edu.uci.ics.hyracks.storage.am.rtree.frames;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.frames.TreeIndexNSMFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import edu.uci.ics.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public class NSMRTreeFrame extends TreeIndexNSMFrame implements IRTreeFrame {

    public ITreeIndexTupleReference[] tuples;
    private final IBinaryComparator intComp = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();

    public NSMRTreeFrame(ITreeIndexTupleWriter tupleWriter) {
        super(tupleWriter, new UnorderedSlotManager());
        this.tuples = new ITreeIndexTupleReference[4]; // change this to number
                                                       // of dim * 2
        for (int i = 0; i < 4; i++) {
            this.tuples[i] = tupleWriter.createTupleReference();
        }
    }
    
    public ITreeIndexTupleReference[] getTuples() {
        return tuples;
    }

    public void setTuples(ITreeIndexTupleReference[] tuples) {
        this.tuples = tuples;
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
    public int split(IRTreeFrame rightFrame, ITupleReference tuple, MultiComparator cmp, RTreeSplitKey leftSplitKey,
            RTreeSplitKey rightSplitKey) throws Exception {

        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame = ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());
        frameTuple.setFieldCount(cmp.getFieldCount());
        rightFrame.setPageTupleFieldCount(cmp.getFieldCount());

        ByteBuffer right = rightFrame.getBuffer();
        int tupleCount = getTupleCount();

        int tuplesToLeft;
        int mid = tupleCount / 2;
        IRTreeFrame targetFrame = null;
        int tupleOff = slotManager.getTupleOff(slotManager.getSlotOff(mid));
        frameTuple.resetByTupleOffset(buf, tupleOff);
        if (cmp.compare(tuple, frameTuple) >= 0) {
            tuplesToLeft = mid + (tupleCount % 2);
            targetFrame = rightFrame;
        } else {
            tuplesToLeft = mid;
            targetFrame = this;
        }
        int tuplesToRight = tupleCount - tuplesToLeft;

        // copy entire page
        System.arraycopy(buf.array(), 0, right.array(), 0, buf.capacity());

        // on right page we need to copy rightmost slots to left
        int src = rightFrame.getSlotManager().getSlotEndOff();
        int dest = rightFrame.getSlotManager().getSlotEndOff() + tuplesToLeft
                * rightFrame.getSlotManager().getSlotSize();
        int length = rightFrame.getSlotManager().getSlotSize() * tuplesToRight;
        System.arraycopy(right.array(), src, right.array(), dest, length);
        right.putInt(tupleCountOff, tuplesToRight);

        // on left page only change the tupleCount indicator
        buf.putInt(tupleCountOff, tuplesToLeft);

        // compact both pages
        rightFrame.compact(cmp);
        compact(cmp);

        // insert last key
        targetFrame.insert(tuple, cmp);

        // set split key to be highest value in left page
        // TODO: find a better way to find the key size
        tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);

        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, cmp.getKeyFieldCount());
        leftSplitKey.initData(splitKeySize);
        this.adjustNode(tuples, cmp);
        rTreeTupleWriterLeftFrame.writeTupleFields(tuples, 0, leftSplitKey.getBuffer(), 0);
        leftSplitKey.getTuple().resetByTupleOffset(leftSplitKey.getBuffer(), 0);

        rightSplitKey.initData(splitKeySize);
        rightFrame.adjustNode(((NSMRTreeFrame) rightFrame).getTuples(), cmp);
        rTreeTupleWriterRightFrame.writeTupleFields(((NSMRTreeFrame) rightFrame).getTuples(), 0, rightSplitKey.getBuffer(), 0);
        rightSplitKey.getTuple().resetByTupleOffset(rightSplitKey.getBuffer(), 0);

        return 0;
    }

    @Override
    public void insertSorted(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
        try {
            insert(tuple, cmp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getChildPageId(ITupleReference tuple, MultiComparator cmp) {
        // find least overlap enlargement, use minimum enlarged area to
        // break tie, if tie still exists use minimum area to break it

        int bestChild = 0;
        double minEnlargedArea = Double.MAX_VALUE;

        // if (getLevel() == 1) {
        for (int i = 0; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);
            double enlargedArea = enlargedArea(frameTuple, tuple, cmp);
            if (enlargedArea < minEnlargedArea) {
                minEnlargedArea = enlargedArea;
                bestChild = i;
            }

            else if (enlargedArea == minEnlargedArea) {
                double area = area(frameTuple, cmp);
                frameTuple.resetByTupleIndex(this, bestChild);
                double bestArea = area(frameTuple, cmp);
                if (area < bestArea) {
                    bestChild = i;
                }
            }
        }
        // } else { // find minimum enlarged area, use minimum area to break
        // tie

        // }
        frameTuple.resetByTupleIndex(this, bestChild);
        if (minEnlargedArea > 0.0) {
            enlarge(frameTuple, tuple, cmp);
        }

        // return the page id of the bestChild tuple
        return buf.getInt(frameTuple.getFieldStart(cmp.getKeyFieldCount()));
    }

    public double area(ITupleReference tuple, MultiComparator cmp) {
        double area = 1.0;
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            area *= DoubleSerializerDeserializer.getDouble(tuple.getFieldData(j), tuple.getFieldStart(j))
                    - DoubleSerializerDeserializer.getDouble(tuple.getFieldData(i), tuple.getFieldStart(i));
        }
        return area;
    }

    public double enlargedArea(ITupleReference tuple, ITupleReference tupleToBeInserted, MultiComparator cmp) {
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

    public void enlarge(ITupleReference tuple, ITupleReference tupleToBeInserted, MultiComparator cmp) {
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                    tupleToBeInserted.getFieldLength(i));
            if (c > 0) {
                System.arraycopy(tupleToBeInserted.getFieldData(i), tupleToBeInserted.getFieldStart(i),
                        tuple.getFieldData(i), tuple.getFieldStart(i), tupleToBeInserted.getFieldLength(i));
            }
            c = cmp.getComparators()[j].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                    tupleToBeInserted.getFieldLength(j));
            if (c < 0) {
                System.arraycopy(tupleToBeInserted.getFieldData(j), tupleToBeInserted.getFieldStart(j),
                        tuple.getFieldData(j), tuple.getFieldStart(j), tupleToBeInserted.getFieldLength(j));
            }
        }
    }

    @Override
    public void adjustTuple(ITupleReference tuple, MultiComparator cmp) {
        frameTuple.setFieldCount(cmp.getFieldCount());
        for (int i = 0; i < getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(this, i);

            int c = intComp.compare(frameTuple.getFieldData(cmp.getKeyFieldCount()),
                    frameTuple.getFieldStart(cmp.getKeyFieldCount()),
                    frameTuple.getFieldLength(cmp.getKeyFieldCount()), tuple.getFieldData(cmp.getKeyFieldCount()),
                    tuple.getFieldStart(cmp.getKeyFieldCount()), tuple.getFieldLength(cmp.getKeyFieldCount()));
            if (c == 0) {
                tupleWriter.writeTuple(tuple, buf, getTupleOffset(i));
                break;
            }
        }
    }

    @Override
    public void adjustNode(ITreeIndexTupleReference[] tuples, MultiComparator cmp) {
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
    public int split(ITreeIndexFrame rightFrame, ITupleReference tuple, MultiComparator cmp, ISplitKey splitKey)
            throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }
}
