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

package org.apache.hyracks.storage.am.btree.compressors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.api.IPrefixSlotManager;
import org.apache.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrame;
import org.apache.hyracks.storage.am.btree.impls.BTreeFieldPrefixTupleReference;
import org.apache.hyracks.storage.am.btree.impls.FieldPrefixSlotManager;
import org.apache.hyracks.storage.am.btree.tuples.BTreeTypeAwareTupleWriter;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameCompressor;
import org.apache.hyracks.storage.common.MultiComparator;

public class FieldPrefixCompressor implements ITreeIndexFrameCompressor {

    // minimum ratio of uncompressed tuples to total tuple to consider re-compression
    private final float ratioThreshold;

    // minimum number of tuple matching field prefixes to consider compressing them
    private final int occurrenceThreshold;

    private final ITypeTraits[] typeTraits;

    public FieldPrefixCompressor(ITypeTraits[] typeTraits, float ratioThreshold, int occurrenceThreshold) {
        this.typeTraits = typeTraits;
        this.ratioThreshold = ratioThreshold;
        this.occurrenceThreshold = occurrenceThreshold;
    }

    @Override
    public boolean compress(ITreeIndexFrame indexFrame, MultiComparator cmp) throws Exception {
        BTreeFieldPrefixNSMLeafFrame frame = (BTreeFieldPrefixNSMLeafFrame) indexFrame;
        int tupleCount = frame.getTupleCount();
        if (tupleCount <= 0) {
            frame.setPrefixTupleCount(0);
            frame.setFreeSpaceOff(frame.getOrigFreeSpaceOff());
            frame.setTotalFreeSpace(frame.getOrigTotalFreeSpace());
            return false;
        }

        if (cmp.getKeyFieldCount() == 1) {
            return false;
        }

        int uncompressedTupleCount = frame.getUncompressedTupleCount();
        float ratio = (float) uncompressedTupleCount / (float) tupleCount;
        if (ratio < ratioThreshold)
            return false;

        IBinaryComparator[] cmps = cmp.getComparators();
        int fieldCount = typeTraits.length;

        ByteBuffer buf = frame.getBuffer();
        byte[] pageArray = buf.array();
        IPrefixSlotManager slotManager = frame.getSlotManager();

        // perform analysis pass
        ArrayList<KeyPartition> keyPartitions = getKeyPartitions(frame, cmp, occurrenceThreshold);
        if (keyPartitions.size() == 0)
            return false;

        // for each keyPartition, determine the best prefix length for
        // compression, and count how many prefix tuple we would need in total
        int totalSlotsNeeded = 0;
        int totalPrefixBytes = 0;
        for (KeyPartition kp : keyPartitions) {

            for (int j = 0; j < kp.pmi.length; j++) {
                int benefitMinusCost = kp.pmi[j].spaceBenefit - kp.pmi[j].spaceCost;
                if (benefitMinusCost > kp.maxBenefitMinusCost) {
                    kp.maxBenefitMinusCost = benefitMinusCost;
                    kp.maxPmiIndex = j;
                }
            }

            // ignore keyPartitions with no benefit and don't count bytes and slots needed
            if (kp.maxBenefitMinusCost <= 0)
                continue;

            totalPrefixBytes += kp.pmi[kp.maxPmiIndex].prefixBytes;
            totalSlotsNeeded += kp.pmi[kp.maxPmiIndex].prefixSlotsNeeded;
        }

        // we use a greedy heuristic to solve this "knapsack"-like problem
        // (every keyPartition has a space savings and a number of slots
        // required, but the number of slots are constrained by MAX_PREFIX_SLOTS)
        // we sort the keyPartitions by maxBenefitMinusCost / prefixSlotsNeeded
        // and later choose the top MAX_PREFIX_SLOTS
        int[] newPrefixSlots;
        if (totalSlotsNeeded > FieldPrefixSlotManager.MAX_PREFIX_SLOTS) {
            // order keyPartitions by the heuristic function
            SortByHeuristic heuristicComparator = new SortByHeuristic();
            Collections.sort(keyPartitions, heuristicComparator);
            int slotsUsed = 0;
            int numberKeyPartitions = -1;
            for (int i = 0; i < keyPartitions.size(); i++) {
                KeyPartition kp = keyPartitions.get(i);
                slotsUsed += kp.pmi[kp.maxPmiIndex].prefixSlotsNeeded;
                if (slotsUsed > FieldPrefixSlotManager.MAX_PREFIX_SLOTS) {
                    numberKeyPartitions = i + 1;
                    slotsUsed -= kp.pmi[kp.maxPmiIndex].prefixSlotsNeeded;
                    break;
                }
            }
            newPrefixSlots = new int[slotsUsed];

            // remove irrelevant keyPartitions and adjust total prefix bytes
            while (keyPartitions.size() >= numberKeyPartitions) {
                int lastIndex = keyPartitions.size() - 1;
                KeyPartition kp = keyPartitions.get(lastIndex);
                if (kp.maxBenefitMinusCost > 0)
                    totalPrefixBytes -= kp.pmi[kp.maxPmiIndex].prefixBytes;
                keyPartitions.remove(lastIndex);
            }

            // re-order keyPartitions by prefix (corresponding to original order)
            SortByOriginalRank originalRankComparator = new SortByOriginalRank();
            Collections.sort(keyPartitions, originalRankComparator);
        } else {
            newPrefixSlots = new int[totalSlotsNeeded];
        }

        int[] newTupleSlots = new int[tupleCount];

        // WARNING: our hope is that compression is infrequent
        // here we allocate a big chunk of memory to temporary hold the new, re-compressed tuple
        // in general it is very hard to avoid this step
        int prefixFreeSpace = frame.getOrigFreeSpaceOff();
        int tupleFreeSpace = prefixFreeSpace + totalPrefixBytes;
        byte[] buffer = new byte[buf.capacity()];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

        // perform compression, and reorg
        // we assume that the keyPartitions are sorted by the prefixes
        // (i.e., in the logical target order)
        int kpIndex = 0;
        int tupleIndex = 0;
        int prefixTupleIndex = 0;
        uncompressedTupleCount = 0;

        BTreeTypeAwareTupleWriter tupleWriter = new BTreeTypeAwareTupleWriter(typeTraits, false);
        BTreeFieldPrefixTupleReference tupleToWrite =
                new BTreeFieldPrefixTupleReference(tupleWriter.createTupleReference());
        tupleToWrite.setFieldCount(fieldCount);

        while (tupleIndex < tupleCount) {
            if (kpIndex < keyPartitions.size()) {

                // beginning of keyPartition found, compress entire keyPartition
                if (tupleIndex == keyPartitions.get(kpIndex).firstTupleIndex) {

                    // number of fields we decided to use for compression of this keyPartition
                    int fieldCountToCompress = keyPartitions.get(kpIndex).maxPmiIndex + 1;
                    int segmentStart = keyPartitions.get(kpIndex).firstTupleIndex;
                    int tuplesInSegment = 1;

                    BTreeFieldPrefixTupleReference prevTuple =
                            new BTreeFieldPrefixTupleReference(tupleWriter.createTupleReference());
                    prevTuple.setFieldCount(fieldCount);

                    BTreeFieldPrefixTupleReference tuple =
                            new BTreeFieldPrefixTupleReference(tupleWriter.createTupleReference());
                    tuple.setFieldCount(fieldCount);

                    for (int i = tupleIndex + 1; i <= keyPartitions.get(kpIndex).lastTupleIndex; i++) {
                        prevTuple.resetByTupleIndex(frame, i - 1);
                        tuple.resetByTupleIndex(frame, i);

                        // check if tuples match in fieldCountToCompress of their first fields
                        int prefixFieldsMatch = 0;
                        for (int j = 0; j < fieldCountToCompress; j++) {
                            if (cmps[j].compare(pageArray, prevTuple.getFieldStart(j), prevTuple.getFieldLength(j),
                                    pageArray, tuple.getFieldStart(j), tuple.getFieldLength(j)) == 0)
                                prefixFieldsMatch++;
                            else
                                break;
                        }

                        // the two tuples must match in exactly the number of fields we decided
                        // to compress for this keyPartition
                        int processSegments = 0;
                        if (prefixFieldsMatch == fieldCountToCompress)
                            tuplesInSegment++;
                        else
                            processSegments++;

                        if (i == keyPartitions.get(kpIndex).lastTupleIndex)
                            processSegments++;

                        for (int r = 0; r < processSegments; r++) {
                            // compress current segment and then start new segment
                            if (tuplesInSegment < occurrenceThreshold || fieldCountToCompress <= 0) {
                                // segment does not have at least occurrenceThreshold tuples, so
                                // write tuples uncompressed
                                for (int j = 0; j < tuplesInSegment; j++) {
                                    int slotNum = segmentStart + j;
                                    tupleToWrite.resetByTupleIndex(frame, slotNum);
                                    newTupleSlots[tupleCount - 1 - slotNum] = slotManager.encodeSlotFields(
                                            FieldPrefixSlotManager.TUPLE_UNCOMPRESSED, tupleFreeSpace);
                                    tupleFreeSpace += tupleWriter.writeTuple(tupleToWrite, byteBuffer, tupleFreeSpace);
                                }
                                uncompressedTupleCount += tuplesInSegment;
                            } else {
                                // segment has enough tuples: compress segment, extract prefix,
                                // write prefix tuple to buffer, and set prefix slot
                                newPrefixSlots[newPrefixSlots.length - 1 - prefixTupleIndex] =
                                        slotManager.encodeSlotFields(fieldCountToCompress, prefixFreeSpace);
                                prefixFreeSpace += tupleWriter.writeTupleFields(prevTuple, 0, fieldCountToCompress,
                                        byteBuffer.array(), prefixFreeSpace);

                                // truncate tuples, write them to buffer, and set tuple slots
                                for (int j = 0; j < tuplesInSegment; j++) {
                                    int currTupleIndex = segmentStart + j;
                                    tupleToWrite.resetByTupleIndex(frame, currTupleIndex);
                                    newTupleSlots[tupleCount - 1 - currTupleIndex] =
                                            slotManager.encodeSlotFields(prefixTupleIndex, tupleFreeSpace);
                                    tupleFreeSpace += tupleWriter.writeTupleFields(tupleToWrite, fieldCountToCompress,
                                            fieldCount - fieldCountToCompress, byteBuffer.array(), tupleFreeSpace);
                                }

                                prefixTupleIndex++;
                            }

                            // begin new segment
                            segmentStart = i;
                            tuplesInSegment = 1;
                        }
                    }

                    tupleIndex = keyPartitions.get(kpIndex).lastTupleIndex;
                    kpIndex++;
                } else {
                    // just write the tuple uncompressed
                    tupleToWrite.resetByTupleIndex(frame, tupleIndex);
                    newTupleSlots[tupleCount - 1 - tupleIndex] =
                            slotManager.encodeSlotFields(FieldPrefixSlotManager.TUPLE_UNCOMPRESSED, tupleFreeSpace);
                    tupleFreeSpace += tupleWriter.writeTuple(tupleToWrite, byteBuffer, tupleFreeSpace);
                    uncompressedTupleCount++;
                }
            } else {
                // just write the tuple uncompressed
                tupleToWrite.resetByTupleIndex(frame, tupleIndex);
                newTupleSlots[tupleCount - 1 - tupleIndex] =
                        slotManager.encodeSlotFields(FieldPrefixSlotManager.TUPLE_UNCOMPRESSED, tupleFreeSpace);
                tupleFreeSpace += tupleWriter.writeTuple(tupleToWrite, byteBuffer, tupleFreeSpace);
                uncompressedTupleCount++;
            }
            tupleIndex++;
        }

        // sanity check to see if we have written exactly as many prefix bytes as computed before
        if (prefixFreeSpace != frame.getOrigFreeSpaceOff() + totalPrefixBytes) {
            throw new Exception("ERROR: Number of prefix bytes written don't match computed number");
        }

        // in some rare instances our procedure could even increase the space requirement which is very dangerous
        // this can happen to to the greedy solution of the knapsack-like problem
        // therefore, we check if the new space exceeds the page size to avoid the only danger of
        // an increasing space
        int totalSpace = tupleFreeSpace + newTupleSlots.length * slotManager.getSlotSize()
                + newPrefixSlots.length * slotManager.getSlotSize();
        if (totalSpace > buf.capacity())
            // just leave the page as is
            return false;

        // copy new tuple and new slots into original page
        int freeSpaceAfterInit = frame.getOrigFreeSpaceOff();
        System.arraycopy(buffer, freeSpaceAfterInit, pageArray, freeSpaceAfterInit,
                tupleFreeSpace - freeSpaceAfterInit);

        // copy prefix slots
        int slotOffRunner = buf.capacity() - slotManager.getSlotSize();
        for (int i = 0; i < newPrefixSlots.length; i++) {
            buf.putInt(slotOffRunner, newPrefixSlots[newPrefixSlots.length - 1 - i]);
            slotOffRunner -= slotManager.getSlotSize();
        }

        // copy tuple slots
        for (int i = 0; i < newTupleSlots.length; i++) {
            buf.putInt(slotOffRunner, newTupleSlots[newTupleSlots.length - 1 - i]);
            slotOffRunner -= slotManager.getSlotSize();
        }

        // update space fields, TODO: we need to update more fields
        frame.setFreeSpaceOff(tupleFreeSpace);
        frame.setPrefixTupleCount(newPrefixSlots.length);
        frame.setUncompressedTupleCount(uncompressedTupleCount);
        int totalFreeSpace = buf.capacity() - tupleFreeSpace
                - ((newTupleSlots.length + newPrefixSlots.length) * slotManager.getSlotSize());
        frame.setTotalFreeSpace(totalFreeSpace);

        return true;
    }

    // we perform an analysis pass over the tuples to determine the costs and
    // benefits of different compression options
    // a "keypartition" is a range of tuples that has an identical first field
    // for each keypartition we chose a prefix length to use for compression
    // i.e., all tuples in a keypartition will be compressed based on the same
    // prefix length (number of fields)
    // the prefix length may be different for different keypartitions
    // the occurrenceThreshold determines the minimum number of tuples that must
    // share a common prefix in order for us to consider compressing them
    private ArrayList<KeyPartition> getKeyPartitions(BTreeFieldPrefixNSMLeafFrame frame, MultiComparator cmp,
            int occurrenceThreshold) throws HyracksDataException {
        IBinaryComparator[] cmps = cmp.getComparators();
        int fieldCount = typeTraits.length;

        int maxCmps = cmps.length - 1;
        ByteBuffer buf = frame.getBuffer();
        byte[] pageArray = buf.array();
        IPrefixSlotManager slotManager = frame.getSlotManager();

        ArrayList<KeyPartition> keyPartitions = new ArrayList<KeyPartition>();
        KeyPartition kp = new KeyPartition(maxCmps);
        keyPartitions.add(kp);

        BTreeTypeAwareTupleWriter tupleWriter = new BTreeTypeAwareTupleWriter(typeTraits, false);

        BTreeFieldPrefixTupleReference prevTuple =
                new BTreeFieldPrefixTupleReference(tupleWriter.createTupleReference());
        prevTuple.setFieldCount(fieldCount);

        BTreeFieldPrefixTupleReference tuple = new BTreeFieldPrefixTupleReference(tupleWriter.createTupleReference());
        tuple.setFieldCount(fieldCount);

        kp.firstTupleIndex = 0;
        int tupleCount = frame.getTupleCount();
        for (int i = 1; i < tupleCount; i++) {
            prevTuple.resetByTupleIndex(frame, i - 1);
            tuple.resetByTupleIndex(frame, i);

            int prefixFieldsMatch = 0;
            for (int j = 0; j < maxCmps; j++) {

                if (cmps[j].compare(pageArray, prevTuple.getFieldStart(j), prevTuple.getFieldLength(j), pageArray,
                        tuple.getFieldStart(j), prevTuple.getFieldLength(j)) == 0) {
                    prefixFieldsMatch++;
                    kp.pmi[j].matches++;

                    int prefixBytes = tupleWriter.bytesRequired(tuple, 0, prefixFieldsMatch);
                    int spaceBenefit = tupleWriter.bytesRequired(tuple) - tupleWriter.bytesRequired(tuple,
                            prefixFieldsMatch, tuple.getFieldCount() - prefixFieldsMatch);

                    if (kp.pmi[j].matches == occurrenceThreshold) {
                        // if we compress this prefix, we pay the cost of storing it once, plus
                        // the size for one prefix slot
                        kp.pmi[j].prefixBytes += prefixBytes;
                        kp.pmi[j].spaceCost += prefixBytes + slotManager.getSlotSize();
                        kp.pmi[j].prefixSlotsNeeded++;
                        kp.pmi[j].spaceBenefit += occurrenceThreshold * spaceBenefit;
                    } else if (kp.pmi[j].matches > occurrenceThreshold) {
                        // we are beyond the occurrence threshold, every additional tuple with a
                        // matching prefix increases the benefit
                        kp.pmi[j].spaceBenefit += spaceBenefit;
                    }
                } else {
                    kp.pmi[j].matches = 1;
                    break;
                }
            }

            // this means not even the first field matched, so we start to consider a new "key partition"
            if (maxCmps > 0 && prefixFieldsMatch == 0) {
                kp.lastTupleIndex = i - 1;

                // remove keyPartitions that don't have enough tuples
                if ((kp.lastTupleIndex - kp.firstTupleIndex) + 1 < occurrenceThreshold)
                    keyPartitions.remove(keyPartitions.size() - 1);

                kp = new KeyPartition(maxCmps);
                keyPartitions.add(kp);
                kp.firstTupleIndex = i;
            }
        }
        kp.lastTupleIndex = tupleCount - 1;
        // remove keyPartitions that don't have enough tuples
        if ((kp.lastTupleIndex - kp.firstTupleIndex) + 1 < occurrenceThreshold)
            keyPartitions.remove(keyPartitions.size() - 1);

        return keyPartitions;
    }

    private class PrefixMatchInfo {
        public int matches = 1;
        public int spaceCost = 0;
        public int spaceBenefit = 0;
        public int prefixSlotsNeeded = 0;
        public int prefixBytes = 0;
    }

    private class KeyPartition {
        public int firstTupleIndex;
        public int lastTupleIndex;
        public PrefixMatchInfo[] pmi;

        public int maxBenefitMinusCost = 0;
        public int maxPmiIndex = -1;

        // number of fields used for compression for this kp of current page
        public KeyPartition(int numKeyFields) {
            pmi = new PrefixMatchInfo[numKeyFields];
            for (int i = 0; i < numKeyFields; i++) {
                pmi[i] = new PrefixMatchInfo();
            }
        }
    }

    private class SortByHeuristic implements Comparator<KeyPartition> {
        @Override
        public int compare(KeyPartition a, KeyPartition b) {
            if (a.maxPmiIndex < 0) {
                if (b.maxPmiIndex < 0)
                    return 0;
                return 1;
            } else if (b.maxPmiIndex < 0)
                return -1;

            // non-negative maxPmiIndex, meaning a non-zero benefit exists
            float thisHeuristicVal = (float) a.maxBenefitMinusCost / (float) a.pmi[a.maxPmiIndex].prefixSlotsNeeded;
            float otherHeuristicVal = (float) b.maxBenefitMinusCost / (float) b.pmi[b.maxPmiIndex].prefixSlotsNeeded;
            if (thisHeuristicVal < otherHeuristicVal)
                return 1;
            else if (thisHeuristicVal > otherHeuristicVal)
                return -1;
            else
                return 0;
        }
    }

    private class SortByOriginalRank implements Comparator<KeyPartition> {
        @Override
        public int compare(KeyPartition a, KeyPartition b) {
            return a.firstTupleIndex - b.firstTupleIndex;
        }
    }
}
