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

package org.apache.hyracks.storage.am.lsm.invertedindex.search;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.IObjectFactory;
import org.apache.hyracks.storage.am.lsm.invertedindex.api.InvertedListCursor;
import org.apache.hyracks.storage.am.lsm.invertedindex.util.ObjectCache;

/**
 * This class contains multiple inverted list partitions. Each partition can contain inverted list cursors in it.
 *
 */
public class InvertedListPartitions {
    private final int DEFAULT_NUM_PARTITIONS = 10;
    private final int PARTITIONS_SLACK_SIZE = 10;
    private final int OBJECT_CACHE_INIT_SIZE = 10;
    private final int OBJECT_CACHE_EXPAND_SIZE = 10;
    private final IObjectFactory<ArrayList<InvertedListCursor>> arrayListFactory;
    private final ObjectCache<ArrayList<InvertedListCursor>> arrayListCache;
    private ArrayList<InvertedListCursor>[] partitions;
    private short minValidPartitionIndex;
    private short maxValidPartitionIndex;

    public InvertedListPartitions() throws HyracksDataException {
        this.arrayListFactory = new ArrayListFactory<InvertedListCursor>();
        this.arrayListCache = new ObjectCache<ArrayList<InvertedListCursor>>(arrayListFactory, OBJECT_CACHE_INIT_SIZE,
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
            partitions = (ArrayList<InvertedListCursor>[]) new ArrayList[initialSize];
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

    public void addInvertedListCursor(InvertedListCursor listCursor, short numTokens) throws HyracksDataException {
        if (numTokens + 1 >= partitions.length) {
            partitions = Arrays.copyOf(partitions, numTokens + PARTITIONS_SLACK_SIZE);
        }
        ArrayList<InvertedListCursor> partitionCursors = partitions[numTokens];
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

    public ArrayList<InvertedListCursor>[] getPartitions() {
        return partitions;
    }

    public short getMinValidPartitionIndex() {
        return minValidPartitionIndex;
    }

    public short getMaxValidPartitionIndex() {
        return maxValidPartitionIndex;
    }
}
