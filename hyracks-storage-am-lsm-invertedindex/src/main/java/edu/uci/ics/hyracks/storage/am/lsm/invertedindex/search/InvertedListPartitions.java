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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.search;

import java.util.ArrayList;
import java.util.Arrays;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk.OnDiskInvertedIndex;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.ObjectCache;

public class InvertedListPartitions {
    private final int PARTITIONING_NUM_TOKENS_FIELD = 1;
    private final int DEFAULT_NUM_PARTITIONS = 10;
    private final int PARTITIONS_SLACK_SIZE = 10;    
    private final OnDiskInvertedIndex invIndex;    
    private final ObjectCache<IInvertedListCursor> invListCursorCache;
    private final ObjectCache<ArrayList<IInvertedListCursor>> arrayListCache;
    private ArrayList<IInvertedListCursor>[] partitions;
    private int minValidPartitionIndex;
    private int maxValidPartitionIndex;

    public InvertedListPartitions(OnDiskInvertedIndex invIndex, ObjectCache<IInvertedListCursor> invListCursorCache,
            ObjectCache<ArrayList<IInvertedListCursor>> arrayListCache) {
        this.invIndex = invIndex;
        this.invListCursorCache = invListCursorCache;
        this.arrayListCache = arrayListCache;
    }

    @SuppressWarnings("unchecked")
    public void reset(int numTokensLowerBound, int numTokensUpperBound) {
        if (partitions == null) {
            int initialSize;
            if (numTokensUpperBound < 0) {
                initialSize = DEFAULT_NUM_PARTITIONS;
            } else {
                initialSize = numTokensUpperBound + 1;
            }
            partitions = (ArrayList<IInvertedListCursor>[]) new ArrayList[initialSize];
        } else {
            if (numTokensUpperBound + 1 >= partitions.length) {
                partitions = Arrays.copyOf(partitions, numTokensUpperBound + 1);
            }
            Arrays.fill(partitions, null);
        }
        invListCursorCache.reset();
        arrayListCache.reset();
        minValidPartitionIndex = Integer.MAX_VALUE;
        maxValidPartitionIndex = Integer.MIN_VALUE;
    }

    public void addInvertedListCursor(ITupleReference btreeTuple) {
        IInvertedListCursor listCursor = invListCursorCache.getNext();
        invIndex.resetInvertedListCursor(btreeTuple, listCursor);
        int numTokens = IntegerSerializerDeserializer.getInt(btreeTuple.getFieldData(PARTITIONING_NUM_TOKENS_FIELD),
                btreeTuple.getFieldStart(PARTITIONING_NUM_TOKENS_FIELD));
        if (numTokens + 1 >= partitions.length) {
            partitions = Arrays.copyOf(partitions, numTokens + PARTITIONS_SLACK_SIZE);
        }
        ArrayList<IInvertedListCursor> partitionCursors = partitions[numTokens];
        if (partitionCursors == null) {
            partitionCursors = arrayListCache.getNext();
            partitionCursors.clear();
            partitions[numTokens] = partitionCursors;
            // Update range of valid partitions.
            if (numTokens < minValidPartitionIndex) {
                minValidPartitionIndex = numTokens;
            }
            if (numTokens > maxValidPartitionIndex) {
                maxValidPartitionIndex = numTokens;
            }
        }
        partitionCursors.add(listCursor);
    }

    public ArrayList<IInvertedListCursor>[] getPartitions() {
        return partitions;
    }

    public int getMinValidPartitionIndex() {
        return minValidPartitionIndex;
    }

    public int getMaxValidPartitionIndex() {
        return maxValidPartitionIndex;
    }
}