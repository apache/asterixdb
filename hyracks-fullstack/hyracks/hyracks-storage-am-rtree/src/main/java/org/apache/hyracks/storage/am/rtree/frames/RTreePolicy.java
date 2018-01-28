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
import org.apache.hyracks.storage.am.rtree.impls.RTreeSplitKey;
import org.apache.hyracks.storage.am.rtree.impls.Rectangle;
import org.apache.hyracks.storage.am.rtree.impls.UnorderedSlotManager;
import org.apache.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriter;
import org.apache.hyracks.storage.common.MultiComparator;

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

    @Override
    public void split(ITreeIndexFrame leftFrame, ByteBuffer buf, ITreeIndexFrame rightFrame, ISlotManager slotManager,
            ITreeIndexTupleReference frameTuple, ITupleReference tuple, ISplitKey splitKey)
            throws HyracksDataException {
        RTreeSplitKey rTreeSplitKey = ((RTreeSplitKey) splitKey);
        RTreeTypeAwareTupleWriter rTreeTupleWriterLeftFrame = ((RTreeTypeAwareTupleWriter) tupleWriter);
        RTreeTypeAwareTupleWriter rTreeTupleWriterRightFrame =
                ((RTreeTypeAwareTupleWriter) rightFrame.getTupleWriter());

        RTreeNSMFrame leftRTreeFrame = ((RTreeNSMFrame) leftFrame);

        double separation = Double.NEGATIVE_INFINITY;
        int seed1 = 0, seed2 = 0;
        int maxFieldPos = keyValueProviders.length / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            frameTuple.resetByTupleIndex(leftRTreeFrame, 0);
            double leastLowerValue =
                    keyValueProviders[i].getValue(frameTuple.getFieldData(i), frameTuple.getFieldStart(i));
            double greatestUpperValue =
                    keyValueProviders[j].getValue(frameTuple.getFieldData(j), frameTuple.getFieldStart(j));
            double leastUpperValue = leastLowerValue;
            double greatestLowerValue = greatestUpperValue;
            int leastUpperIndex = 0;
            int greatestLowerIndex = 0;
            double width;

            int tupleCount = leftRTreeFrame.getTupleCount();
            for (int k = 1; k < tupleCount; ++k) {
                frameTuple.resetByTupleIndex(leftRTreeFrame, k);
                double lowerValue =
                        keyValueProviders[i].getValue(frameTuple.getFieldData(i), frameTuple.getFieldStart(i));
                if (lowerValue > greatestLowerValue) {
                    greatestLowerIndex = k;
                    cmpFrameTuple.resetByTupleIndex(leftRTreeFrame, k);
                    greatestLowerValue = keyValueProviders[i].getValue(cmpFrameTuple.getFieldData(i),
                            cmpFrameTuple.getFieldStart(i));
                }
                double higherValue =
                        keyValueProviders[j].getValue(frameTuple.getFieldData(j), frameTuple.getFieldStart(j));
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

        if (seed1 == seed2) {
            if (seed1 == 0) {
                seed2 = 1;
            } else {
                --seed2;
            }
        }

        int totalBytes = 0, numOfDeletedTuples = 0;

        frameTuple.resetByTupleIndex(leftRTreeFrame, seed1);
        rec[0].set(frameTuple, keyValueProviders);
        rightFrame.insert(frameTuple, -1);
        ((UnorderedSlotManager) slotManager).modifySlot(slotManager.getSlotOff(seed1), -1);
        totalBytes += leftRTreeFrame.getTupleSize(frameTuple);
        numOfDeletedTuples++;

        frameTuple.resetByTupleIndex(leftRTreeFrame, seed2);
        rec[1].set(frameTuple, keyValueProviders);

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
        buf.putInt(totalFreeSpaceOff,
                buf.getInt(totalFreeSpaceOff) + totalBytes + (slotManager.getSlotSize() * numOfDeletedTuples));

        // compact both pages
        rightFrame.compact();
        leftRTreeFrame.compact();

        // The assumption here is that the new tuple cannot be larger than page
        // size, thus it must fit in either pages.
        if (rec[0].enlargedArea(tuple, keyValueProviders) < rec[1].enlargedArea(tuple, keyValueProviders)) {
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
        rTreeTupleWriterLeftFrame.writeTupleFields(leftRTreeFrame.getMBRTuples(), 0, rTreeSplitKey.getLeftPageBuffer(),
                0);
        rTreeSplitKey.getLeftTuple().resetByTupleOffset(rTreeSplitKey.getLeftPageBuffer().array(), 0);

        ((IRTreeFrame) rightFrame).adjustMBR();
        rTreeTupleWriterRightFrame.writeTupleFields(((RTreeNSMFrame) rightFrame).getMBRTuples(), 0,
                rTreeSplitKey.getRightPageBuffer(), 0);
        rTreeSplitKey.getRightTuple().resetByTupleOffset(rTreeSplitKey.getRightPageBuffer().array(), 0);
    }

    @Override
    public int findBestChildPosition(ITreeIndexFrame frame, ITupleReference tuple, ITreeIndexTupleReference frameTuple,
            MultiComparator cmp) throws HyracksDataException {
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

        return bestChild;
    }

}
