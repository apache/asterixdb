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

package org.apache.asterix.runtime.aggregates.utils;

import java.util.List;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * A hash set containing Pointables.
 * Currently only {@link #add(IPointable)} operation as implemented.
 */
public class PointableHashSet {

    private final IObjectPool<List<IPointable>, ATypeTag> listAllocator;

    private final IBinaryComparator comparator;

    private final IBinaryHashFunction hashFunction;

    private final Int2ObjectMap<List<IPointable>> hashes;

    public PointableHashSet(IObjectPool<List<IPointable>, ATypeTag> listAllocator, IAType itemType) {
        this.listAllocator = listAllocator;
        comparator = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(itemType, itemType, true)
                .createBinaryComparator();
        hashFunction = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(itemType)
                .createBinaryHashFunction();
        hashes = new Int2ObjectOpenHashMap<>();
    }

    public void clear() {
        hashes.clear();
    }

    /**
     * Adds an instance to this set.
     * Returns {@code true} if the set did not already contain this item, {@code false} otherwise.
     */
    public boolean add(IPointable item) throws HyracksDataException {
        // look up if it already exists
        int hash = hashFunction.hash(item.getByteArray(), item.getStartOffset(), item.getLength());
        List<IPointable> sameHashes = hashes.get(hash);
        if (sameHashes == null) {
            // new item
            sameHashes = listAllocator.allocate(null);
            sameHashes.clear();
            sameHashes.add(makeStoredItem(item));
            hashes.put(hash, sameHashes);
            return true;
        } else if (PointableHelper.findItem(item, sameHashes, comparator) == null) {
            // new item, it could happen that two hashes are the same but they are for different items
            sameHashes.add(makeStoredItem(item));
            return true;
        } else {
            return false;
        }
    }

    protected IPointable makeStoredItem(IPointable item) throws HyracksDataException {
        return item;
    }
}