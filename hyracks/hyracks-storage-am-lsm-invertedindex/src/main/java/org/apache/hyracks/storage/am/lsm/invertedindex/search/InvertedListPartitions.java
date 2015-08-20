/*
 * Copyright 2009-2013 by The Regents of the University of California
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

import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListCursor;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.ObjectCache;

public class InvertedListPartitions {
    private final int DEFAULT_NUM_PARTITIONS = 10;
    private final int PARTITIONS_SLACK_SIZE = 10;
    private final int OBJECT_CACHE_INIT_SIZE = 10;
    private final int OBJECT_CACHE_EXPAND_SIZE = 10;
    private final IObjectFactory<ArrayList<IInvertedListCursor>> arrayListFactory;
    private final ObjectCache<ArrayList<IInvertedListCursor>> arrayListCache;
    private ArrayList<IInvertedListCursor>[] partitions;
    private short minValidPartitionIndex;
    private short maxValidPartitionIndex;

    public InvertedListPartitions() {
        this.arrayListFactory = new ArrayListFactory<IInvertedListCursor>();
        this.arrayListCache = new ObjectCache<ArrayList<IInvertedListCursor>>(arrayListFactory, OBJECT_CACHE_INIT_SIZE,
                OBJECT_CACHE_EXPAND_SIZE);
    }

    @SuppressWarnings("unchecked")
    public void reset(short numTokensLowerBound, short numTokensUpperBound) {
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
        arrayListCache.reset();
        minValidPartitionIndex = Short.MAX_VALUE;
        maxValidPartitionIndex = Short.MIN_VALUE;
    }

    public void addInvertedListCursor(IInvertedListCursor listCursor, short numTokens) {
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

    public short getMinValidPartitionIndex() {
        return minValidPartitionIndex;
    }

    public short getMaxValidPartitionIndex() {
        return maxValidPartitionIndex;
    }
}