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
package org.apache.asterix.runtime.evaluators.functions;

import java.util.List;

import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class ArraySymDiffEval extends AbstractArrayProcessArraysEval {
    private final IBinaryHashFunction binaryHashFunction;
    private final Int2ObjectMap<List<ValueCounter>> hashes;
    private final IObjectPool<List<ValueCounter>, ATypeTag> arrayListAllocator;
    private final IObjectPool<ValueCounter, ATypeTag> valueCounterAllocator;
    private final IBinaryComparator comp;
    private final IntArrayList intHashes;

    ArraySymDiffEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, SourceLocation sourceLocation,
            IAType[] argTypes) throws HyracksDataException {
        super(args, ctx, sourceLocation, argTypes);
        arrayListAllocator = new ListObjectPool<>(new ArrayListFactory<>());
        valueCounterAllocator = new ListObjectPool<>(new ValueCounterFactory());
        hashes = new Int2ObjectOpenHashMap<>();
        // for functions that accept multiple lists arguments, they will be casted to open, hence item is ANY
        comp = BinaryComparatorFactoryProvider.INSTANCE
                .getBinaryComparatorFactory(BuiltinType.ANY, BuiltinType.ANY, true).createBinaryComparator();
        intHashes = new IntArrayList(50, 10);
        binaryHashFunction = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(BuiltinType.ANY)
                .createBinaryHashFunction();
    }

    protected class ValueCounter implements IValueReference {
        private IPointable value;
        private int listIndex;
        private int counter;

        ValueCounter() {
        }

        protected void reset(IPointable value, int listIndex, int counter) {
            this.value = value;
            this.listIndex = listIndex;
            this.counter = counter;
        }

        @Override
        public byte[] getByteArray() {
            return value.getByteArray();
        }

        @Override
        public int getStartOffset() {
            return value.getStartOffset();
        }

        @Override
        public int getLength() {
            return value.getLength();
        }
    }

    public class ValueCounterFactory implements IObjectFactory<ValueCounter, ATypeTag> {

        @Override
        public ValueCounter create(ATypeTag arg) {
            return new ValueCounter();
        }
    }

    @Override
    protected void init() {
        hashes.clear();
        intHashes.clear();
    }

    @Override
    protected void finish(IAsterixListBuilder listBuilder) throws HyracksDataException {
        ValueCounter item;
        List<ValueCounter> items;
        // TODO(ali): temp solution to avoid iterator object creation, find a better way
        for (int i = 0; i < intHashes.size(); i++) {
            items = hashes.get(intHashes.get(i));
            for (int k = 0; k < items.size(); k++) {
                item = items.get(k);
                if (checkCounter(item.counter)) {
                    listBuilder.addItem(item.value);
                }
            }
        }
    }

    @Override
    protected void release() {
        arrayListAllocator.reset();
        valueCounterAllocator.reset();
    }

    protected boolean checkCounter(int counter) {
        return counter == 1;
    }

    @Override
    protected boolean processItem(IPointable item, int listIndex, IAsterixListBuilder listBuilder)
            throws HyracksDataException {
        // lookup the item
        int hash = binaryHashFunction.hash(item.getByteArray(), item.getStartOffset(), item.getLength());
        List<ValueCounter> sameHashes = hashes.get(hash);
        if (sameHashes == null) {
            // new item
            sameHashes = arrayListAllocator.allocate(null);
            sameHashes.clear();
            addItem(item, listIndex, sameHashes);
            hashes.put(hash, sameHashes);
            intHashes.add(hash);
            return true;
        } else {
            // potentially, item already exists
            ValueCounter itemListIdxCounter = PointableHelper.findItem(item, sameHashes, comp);
            if (itemListIdxCounter == null) {
                // new item having the same hash as a different item
                addItem(item, listIndex, sameHashes);
                return true;
            }
            // the item already exists, increment the counter (don't increment the counter for the same listIndex)
            if (itemListIdxCounter.listIndex != listIndex) {
                itemListIdxCounter.listIndex = listIndex;
                itemListIdxCounter.counter++;
            }
            // false, since we didn't add (use) the item
            return false;
        }
    }

    private void addItem(IPointable item, int listIndex, List<ValueCounter> sameHashes) {
        ValueCounter valueCounter = valueCounterAllocator.allocate(null);
        valueCounter.reset(item, listIndex, 1);
        sameHashes.add(valueCounter);
    }
}
