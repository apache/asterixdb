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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ISlotManager;
import edu.uci.ics.hyracks.storage.am.common.api.ISplitKey;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeFrame;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreePolicy;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import edu.uci.ics.hyracks.storage.am.rtree.impls.Rectangle;
import edu.uci.ics.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;

public class RTreePolicy implements IRTreePolicy {

    private Rectangle[] rec;

    private final ITreeIndexTupleWriter tupleWriter;
    private final IPrimitiveValueProvider[] keyValueProviders;
    private ITreeIndexTupleReference cmpFrameTuple;
    private final int totalFreeSpaceOff;

    public RTreePolicy(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders,
            ITreeIndexTupleReference cmpFrameTuple, int totalFreeSpaceOff) {
        this.tupleWriter = tupleWriter;
        this.keyValueProviders = keyValueProviders;
        this.cmpFrameTuple = cmpFrameTuple;
        this.totalFreeSpaceOff = totalFreeSpaceOff;

        rec = new Rectangle[2];
        for (int i = 0; i < 2; i++) {
            rec[i] = new Rectangle(keyValueProviders.length / 2);
        }
    }

    public void split(ITreeIndexFrame leftFrame, ByteBuffer buf, ITreeIndexFrame rightFrame, ISlotManager slotManager,
            ITreeIndexTupleReference frameTuple, ITupleReference tuple, ISplitKey splitKey) {
        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame = ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());

        RTreeNSMFrame leftRTreeFrame = ((RTreeNSMFrame) leftFrame);

        double separation = Double.NEGATIVE_INFINITY;
        int seed1 = 0, seed2 = 0;
        int maxFieldPos = keyValueProviders.length / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            frameTuple.resetByTupleIndex(leftRTreeFrame, 0);
            double leastLowerValue = keyValueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i));
            double greatestUpperValue = keyValueProviders[j].getValue(tuple.getFieldData(j), tuple.getFieldStart(j));
            double leastUpperValue = leastLowerValue;
            double greatestLowerValue = greatestUpperValue;
            int leastUpperIndex = -1;
            int greatestLowerIndex = -1;
            double width;

            for (int k = 0; k < leftRTreeFrame.getTupleCount(); ++k) {
                frameTuple.resetByTupleIndex(leftRTreeFrame, k);
                double lowerValue = keyValueProviders[i].getValue(frameTuple.getFieldData(i),
                        frameTuple.getFieldStart(i));
                if (lowerValue > greatestLowerValue) {
                    greatestLowerIndex = k;
                    cmpFrameTuple.resetByTupleIndex(leftRTreeFrame, k);
                    greatestLowerValue = keyValueProviders[i].getValue(cmpFrameTuple.getFieldData(i),
                            cmpFrameTuple.getFieldStart(i));
                }
                double higherValue = keyValueProviders[j].getValue(frameTuple.getFieldData(j),
                        frameTuple.getFieldStart(j));
                if (higherValue < leastUpperValue) {
                    leastUpperIndex = k;
                    cmpFrameTuple.resetByTupleIndex(leftRTreeFrame, k);
                    leastUpperValue = keyValueProviders[j].getValue(cmpFrameTuple.getFieldData(j),
                            cmpFrameTuple.getFieldStart(j));
                }

                leastLowerValue = Math.min(lowerValue, leastLowerValue);
                greatestUpperValue = Math.max(higherValue, greatestUpperValue);
            }

            width = greatestUpperValue - leastLowerValue;
            if (width <= 0) {
                width = 1;
            }

            double f = (greatestLowerValue - leastUpperValue) / width;

            if (f > separation) {
                seed1 = leastUpperIndex;
                seed2 = greatestLowerIndex;
                separation = f;
            }
        }

        if (seed1 == -1 && seed1 == seed2) {
            seed2 = 0;
        } else if (seed1 == seed2) {
            if (seed2 == 0) {
                ++seed2;
            } else {
                --seed2;
            }
        }

        int totalBytes = 0, numOfDeletedTuples = 0;
        if (seed1 == -1) {
            rightFrame.insert(tuple, -1);
            rec[0].set(tuple, keyValueProviders);
        } else {
            frameTuple.resetByTupleIndex(leftRTreeFrame, seed1);
            rec[0].set(frameTuple, keyValueProviders);
            rightFrame.insert(frameTuple, -1);
            ((UnorderedSlotManager) slotManager).modifySlot(slotManager.getSlotOff(seed1), -1);
            totalBytes += leftRTreeFrame.getTupleSize(frameTuple);
            numOfDeletedTuples++;
        }

        if (seed2 == -1) {
            rec[1].set(tuple, keyValueProviders);
        } else {
            frameTuple.resetByTupleIndex(leftRTreeFrame, seed2);
            rec[1].set(frameTuple, keyValueProviders);
        }
        int remainingTuplestoBeInsertedInRightFrame;
        for (int k = 0; k < leftRTreeFrame.getTupleCount(); ++k) {
            remainingTuplestoBeInsertedInRightFrame = leftRTreeFrame.getTupleCount() / 2 - rightFrame.getTupleCount();
            if (remainingTuplestoBeInsertedInRightFrame == 0) {
                break;
            }
            if (k != seed1 && k != seed2) {
                frameTuple.resetByTupleIndex(leftRTreeFrame, k);
                if (rec[0].enlargedArea(frameTuple, keyValueProviders) < rec[1].enlargedArea(frameTuple,
                        keyValueProviders)
                        || leftRTreeFrame.getTupleCount() - k <= remainingTuplestoBeInsertedInRightFrame) {
                    rightFrame.insert(frameTuple, -1);
                    rec[0].enlarge(frameTuple, keyValueProviders);
                    ((UnorderedSlotManager) slotManager).modifySlot(slotManager.getSlotOff(k), -1);
                    totalBytes += leftRTreeFrame.getTupleSize(frameTuple);
                    numOfDeletedTuples++;
                } else {
                    rec[1].enlarge(frameTuple, keyValueProviders);
                }
            }

        }

        ((UnorderedSlotManager) slotManager).deleteEmptySlots();

        // maintain space information
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + totalBytes
                + (slotManager.getSlotSize() * numOfDeletedTuples));

        // compact both pages
        rightFrame.compact();
        leftRTreeFrame.compact();

        if (seed2 == -1) {
            leftRTreeFrame.insert(tuple, -1);
        } else if (seed1 != -1
                && rec[0].enlargedArea(tuple, keyValueProviders) < rec[1].enlargedArea(tuple, keyValueProviders)) {
            rightFrame.insert(tuple, -1);
        } else if (seed1 != -1) {
            leftRTreeFrame.insert(tuple, -1);
        }

        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, keyValueProviders.length);

        splitKey.initData(splitKeySize);
        leftRTreeFrame.adjustMBR();
        rTreeTupleWriterLeftFrame.writeTupleFields(leftRTreeFrame.getTuples(), 0, rTreeSplitKey.getLeftPageBuffer(), 0);
        rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer(), 0);

        ((IRTreeFrame) rightFrame).adjustMBR();
        rTreeTupleWriterRightFrame.writeTupleFields(((RTreeNSMFrame) rightFrame).getTuples(), 0,
                rTreeSplitKey.getRightPageBuffer(), 0);
        rTreeSplitKey.getRightTuple().resetByTupleOffset(rTreeSplitKey.getRightPageBuffer(), 0);

    }

    public boolean findBestChild(ITreeIndexFrame frame, ITupleReference tuple, ITreeIndexTupleReference frameTuple,
            MultiComparator cmp) {
        cmpFrameTuple.setFieldCount(cmp.getKeyFieldCount());
        frameTuple.setFieldCount(cmp.getKeyFieldCount());

        int bestChild = 0;
        double minEnlargedArea = Double.MAX_VALUE;

        // find minimum enlarged area, use minimum area to break tie
        for (int i = 0; i < frame.getTupleCount(); i++) {
            frameTuple.resetByTupleIndex(frame, i);
            double enlargedArea = RTreeComputationUtils.enlargedArea(frameTuple, tuple, cmp, keyValueProviders);
            if (enlargedArea < minEnlargedArea) {
                minEnlargedArea = enlargedArea;
                bestChild = i;
            } else if (enlargedArea == minEnlargedArea) {
                double area = RTreeComputationUtils.area(frameTuple, cmp, keyValueProviders);
                frameTuple.resetByTupleIndex(frame, bestChild);
                double minArea = RTreeComputationUtils.area(frameTuple, cmp, keyValueProviders);
                if (area < minArea) {
                    bestChild = i;
                }
            }
        }

        frameTuple.resetByTupleIndex(frame, bestChild);
        if (minEnlargedArea > 0.0) {
            return true;
        } else {
            return false;
        }
    }

}