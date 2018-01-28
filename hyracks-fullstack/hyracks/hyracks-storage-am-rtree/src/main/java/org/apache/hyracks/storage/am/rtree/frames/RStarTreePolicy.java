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

package org.apache.hyracks.storage.am.rtree.frames;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ISlotManager;
import org.apache.hyracks.storage.am.common.api.ISplitKey;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.frames.FrameOpSpaceStatus;
import org.apache.hyracks.storage.am.rtree.api.IRTreeFrame;
import org.apache.hyracks.storage.am.rtree.api.IRTreePolicy;
import org.apache.hyracks.storage.am.rtree.impls.EntriesOrder;
import org.apache.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import org.apache.hyracks.storage.am.rtree.impls.Rectangle;
import org.apache.hyracks.storage.am.rtree.impls.TupleEntryArrayList;
import org.apache.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;
import org.apache.hyracks.storage.common.MultiComparator;

public class RStarTreePolicy implements IRTreePolicy {

    private TupleEntryArrayList tupleEntries1;
    private TupleEntryArrayList tupleEntries2;
    private Rectangle[] rec;

    private static final int nearMinimumOverlapFactor = 32;
    private static final double splitFactor = 0.4;
    private static final int numTuplesEntries = 100;

    private final ITreeIndexTupleWriter tupleWriter;
    private final IPrimitiveValueProvider[] keyValueProviders;
    private ITreeIndexTupleReference cmpFrameTuple;
    private final int totalFreeSpaceOff;

    public RStarTreePolicy(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders,
            ITreeIndexTupleReference cmpFrameTuple, int totalFreeSpaceOff) {
        this.tupleWriter = tupleWriter;
        this.keyValueProviders = keyValueProviders;
        this.totalFreeSpaceOff = totalFreeSpaceOff;
        this.cmpFrameTuple = cmpFrameTuple;
        tupleEntries1 = new TupleEntryArrayList(numTuplesEntries, numTuplesEntries);
        tupleEntries2 = new TupleEntryArrayList(numTuplesEntries, numTuplesEntries);
        rec = new Rectangle[4];
        for (int i = 0; i < 4; i++) {
            rec[i] = new Rectangle(keyValueProviders.length / 2);
        }
    }

    @Override
    public void split(ITreeIndexFrame leftFrame, ByteBuffer buf, ITreeIndexFrame rightFrame, ISlotManager slotManager,
            ITreeIndexTupleReference frameTuple, ITupleReference tuple, ISplitKey splitKey)
            throws HyracksDataException {
        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterleftRTreeFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame =
                ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());

        RTreeNSMFrame leftRTreeFrame = ((RTreeNSMFrame) leftFrame);

        // calculations are based on the R*-tree paper
        int m = (int) Math.floor((leftRTreeFrame.getTupleCount() + 1) * splitFactor);
        int splitDistribution = leftRTreeFrame.getTupleCount() - (2 * m) + 2;

        // to calculate the minimum margin in order to pick the split axis
        double minMargin = Double.MAX_VALUE;
        int splitAxis = 0, sortOrder = 0;

        int maxFieldPos = keyValueProviders.length / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            for (int k = 0; k < leftRTreeFrame.getTupleCount(); ++k) {

                frameTuple.resetByTupleIndex(leftRTreeFrame, k);
                double LowerKey =
                        keyValueProviders[i].getValue(frameTuple.getFieldData(i), frameTuple.getFieldStart(i));
                double UpperKey =
                        keyValueProviders[j].getValue(frameTuple.getFieldData(j), frameTuple.getFieldStart(j));

                tupleEntries1.add(k, LowerKey);
                tupleEntries2.add(k, UpperKey);
            }
            double LowerKey = keyValueProviders[i].getValue(tuple.getFieldData(i), tuple.getFieldStart(i));
            double UpperKey = keyValueProviders[j].getValue(tuple.getFieldData(j), tuple.getFieldStart(j));

            tupleEntries1.add(-1, LowerKey);
            tupleEntries2.add(-1, UpperKey);

            tupleEntries1.sort(EntriesOrder.ASCENDING, leftRTreeFrame.getTupleCount() + 1);
            tupleEntries2.sort(EntriesOrder.ASCENDING, leftRTreeFrame.getTupleCount() + 1);

            double lowerMargin = 0.0, upperMargin = 0.0;
            // generate distribution
            for (int k = 1; k <= splitDistribution; ++k) {
                int d = m - 1 + k;

                generateDist(leftRTreeFrame, frameTuple, tuple, tupleEntries1, rec[0], 0, d);
                generateDist(leftRTreeFrame, frameTuple, tuple, tupleEntries2, rec[1], 0, d);
                generateDist(leftRTreeFrame, frameTuple, tuple, tupleEntries1, rec[2], d,
                        leftRTreeFrame.getTupleCount() + 1);
                generateDist(leftRTreeFrame, frameTuple, tuple, tupleEntries2, rec[3], d,
                        leftRTreeFrame.getTupleCount() + 1);

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

        for (int i = 0; i < leftRTreeFrame.getTupleCount(); ++i) {
            frameTuple.resetByTupleIndex(leftRTreeFrame, i);
            double key = keyValueProviders[splitAxis + sortOrder].getValue(
                    frameTuple.getFieldData(splitAxis + sortOrder), frameTuple.getFieldStart(splitAxis + sortOrder));
            tupleEntries1.add(i, key);
        }
        double key = keyValueProviders[splitAxis + sortOrder].getValue(tuple.getFieldData(splitAxis + sortOrder),
                tuple.getFieldStart(splitAxis + sortOrder));
        tupleEntries1.add(-1, key);
        tupleEntries1.sort(EntriesOrder.ASCENDING, leftRTreeFrame.getTupleCount() + 1);

        double minArea = Double.MAX_VALUE;
        double minOverlap = Double.MAX_VALUE;
        int splitPoint = 0;
        for (int i = 1; i <= splitDistribution; ++i) {
            int d = m - 1 + i;

            generateDist(leftRTreeFrame, frameTuple, tuple, tupleEntries1, rec[0], 0, d);
            generateDist(leftRTreeFrame, frameTuple, tuple, tupleEntries1, rec[2], d,
                    leftRTreeFrame.getTupleCount() + 1);

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
        if (splitPoint < (leftRTreeFrame.getTupleCount() + 1) / 2) {
            startIndex = 0;
            endIndex = splitPoint;
        } else {
            startIndex = splitPoint;
            endIndex = (leftRTreeFrame.getTupleCount() + 1);
        }
        boolean insertedNewTupleInRightFrame = false;
        int totalBytes = 0, numOfDeletedTuples = 0;
        for (int i = startIndex; i < endIndex; i++) {
            if (tupleEntries1.get(i).getTupleIndex() != -1) {
                frameTuple.resetByTupleIndex(leftRTreeFrame, tupleEntries1.get(i).getTupleIndex());
                rightFrame.insert(frameTuple, -1);
                ((UnorderedSlotManager) slotManager)
                        .modifySlot(slotManager.getSlotOff(tupleEntries1.get(i).getTupleIndex()), -1);
                totalBytes += leftRTreeFrame.getTupleSize(frameTuple);
                numOfDeletedTuples++;
            } else {
                insertedNewTupleInRightFrame = true;
            }
        }

        ((UnorderedSlotManager) slotManager).deleteEmptySlots();

        // maintain space information
        buf.putInt(totalFreeSpaceOff,
                buf.getInt(totalFreeSpaceOff) + totalBytes + (slotManager.getSlotSize() * numOfDeletedTuples));

        // compact both pages
        rightFrame.compact();
        leftRTreeFrame.compact();

        // The assumption here is that the new tuple cannot be larger than page
        // size, thus it must fit in either pages.
        if (insertedNewTupleInRightFrame) {
            if (rightFrame.hasSpaceInsert(tuple) == FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
                rightFrame.insert(tuple, -1);
            } else {
                leftRTreeFrame.insert(tuple, -1);
            }
        } else if (leftRTreeFrame.hasSpaceInsert(tuple) == FrameOpSpaceStatus.SUFFICIENT_CONTIGUOUS_SPACE) {
            leftRTreeFrame.insert(tuple, -1);
        } else {
            rightFrame.insert(tuple, -1);
        }

        int tupleOff = slotManager.getTupleOff(slotManager.getSlotEndOff());
        frameTuple.resetByTupleOffset(buf.array(), tupleOff);
        int splitKeySize = tupleWriter.bytesRequired(frameTuple, 0, keyValueProviders.length);

        splitKey.initData(splitKeySize);
        leftRTreeFrame.adjustMBR();
        rTreeTupleWriterleftRTreeFrame.writeTupleFields(leftRTreeFrame.getMBRTuples(), 0,
                rTreeSplitKey.getLeftPageBuffer(), 0);
        rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer().array(), 0);

        ((IRTreeFrame) rightFrame).adjustMBR();
        rTreeTupleWriterRightFrame.writeTupleFields(((RTreeNSMFrame) rightFrame).getMBRTuples(), 0,
                rTreeSplitKey.getRightPageBuffer(), 0);
        rTreeSplitKey.getRightTuple().resetByTupleOffset(rTreeSplitKey.getRightPageBuffer().array(), 0);

        tupleEntries1.clear();
        tupleEntries2.clear();
    }

    public void generateDist(ITreeIndexFrame leftRTreeFrame, ITreeIndexTupleReference frameTuple, ITupleReference tuple,
            TupleEntryArrayList entries, Rectangle rec, int start, int end) {
        int j = 0;
        while (entries.get(j).getTupleIndex() == -1) {
            j++;
        }
        frameTuple.resetByTupleIndex(leftRTreeFrame, entries.get(j).getTupleIndex());
        rec.set(frameTuple, keyValueProviders);
        for (int i = start; i < end; ++i) {
            if (i != j) {
                if (entries.get(i).getTupleIndex() != -1) {
                    frameTuple.resetByTupleIndex(leftRTreeFrame, entries.get(i).getTupleIndex());
                    rec.enlarge(frameTuple, keyValueProviders);
                } else {
                    rec.enlarge(tuple, keyValueProviders);
                }
            }
        }
    }

    @Override
    public int findBestChildPosition(ITreeIndexFrame frame, ITupleReference tuple, ITreeIndexTupleReference frameTuple,
            MultiComparator cmp) throws HyracksDataException {
        cmpFrameTuple.setFieldCount(cmp.getKeyFieldCount());
        frameTuple.setFieldCount(cmp.getKeyFieldCount());

        int bestChild = 0;
        double minEnlargedArea = Double.MAX_VALUE;

        // the children pointers in the node point to leaves
        if (frame.getLevel() == 1) {
            // find least overlap enlargement, use minimum enlarged area to
            // break tie, if tie still exists use minimum area to break it
            for (int i = 0; i < frame.getTupleCount(); ++i) {
                frameTuple.resetByTupleIndex(frame, i);
                double enlargedArea = RTreeComputationUtils.enlargedArea(frameTuple, tuple, cmp, keyValueProviders);
                tupleEntries1.add(i, enlargedArea);
                if (enlargedArea < minEnlargedArea) {
                    minEnlargedArea = enlargedArea;
                    bestChild = i;
                }
            }
            if (minEnlargedArea < RTreeNSMFrame.doubleEpsilon() || minEnlargedArea > RTreeNSMFrame.doubleEpsilon()) {
                minEnlargedArea = Double.MAX_VALUE;
                int k;
                if (frame.getTupleCount() > nearMinimumOverlapFactor) {
                    // sort the entries based on their area enlargement needed
                    // to include the object
                    tupleEntries1.sort(EntriesOrder.ASCENDING, frame.getTupleCount());
                    k = nearMinimumOverlapFactor;
                } else {
                    k = frame.getTupleCount();
                }

                double minOverlap = Double.MAX_VALUE;
                int id = 0;
                for (int i = 0; i < k; ++i) {
                    double difference = 0.0;
                    for (int j = 0; j < frame.getTupleCount(); ++j) {
                        frameTuple.resetByTupleIndex(frame, j);
                        cmpFrameTuple.resetByTupleIndex(frame, tupleEntries1.get(i).getTupleIndex());

                        int c = ((RTreeNSMInteriorFrame) frame).pointerCmp(frameTuple, cmpFrameTuple, cmp);
                        if (c != 0) {
                            double intersection = RTreeComputationUtils.overlappedArea(frameTuple, tuple, cmpFrameTuple,
                                    cmp, keyValueProviders);
                            if (intersection != 0.0) {
                                difference += intersection - RTreeComputationUtils.overlappedArea(frameTuple, null,
                                        cmpFrameTuple, cmp, keyValueProviders);
                            }
                        } else {
                            id = j;
                        }
                    }

                    double enlargedArea =
                            RTreeComputationUtils.enlargedArea(cmpFrameTuple, tuple, cmp, keyValueProviders);
                    if (difference < minOverlap) {
                        minOverlap = difference;
                        minEnlargedArea = enlargedArea;
                        bestChild = id;
                    } else if (difference == minOverlap) {
                        if (enlargedArea < minEnlargedArea) {
                            minEnlargedArea = enlargedArea;
                            bestChild = id;
                        } else if (enlargedArea == minEnlargedArea) {
                            double area = RTreeComputationUtils.area(cmpFrameTuple, cmp, keyValueProviders);
                            frameTuple.resetByTupleIndex(frame, bestChild);
                            double minArea = RTreeComputationUtils.area(frameTuple, cmp, keyValueProviders);
                            if (area < minArea) {
                                bestChild = id;
                            }
                        }
                    }
                }
            }
        } else { // find minimum enlarged area, use minimum area to break tie
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
        }
        tupleEntries1.clear();

        return bestChild;
    }
}
