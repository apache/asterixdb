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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.utils.ArrayFunctionsUtil;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;

public class ArraySymDiffEval extends AbstractArrayProcessArraysEval {
    private final IBinaryHashFunction binaryHashFunction;
    private final Int2ObjectMap<List<ValueCounter>> hashes;
    private final IObjectPool<List<ValueCounter>, ATypeTag> arrayListAllocator;
    private final IObjectPool<ValueCounter, ATypeTag> valueCounterAllocator;

    public ArraySymDiffEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx, SourceLocation sourceLocation,
            IAType[] argTypes) throws HyracksDataException {
        super(args, ctx, true, sourceLocation, argTypes);
        arrayListAllocator = new ListObjectPool<>(new ArrayListFactory<>());
        valueCounterAllocator = new ListObjectPool<>(new ValueCounterFactory());
        hashes = new Int2ObjectOpenHashMap<>();
        binaryHashFunction = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(null)
                .createBinaryHashFunction();
    }

    protected class ValueCounter implements IValueReference {
        private IPointable value;
        private int listIndex;
        private int counter;

        protected ValueCounter() {
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
    }

    @Override
    protected void finish(IAsterixListBuilder listBuilder) throws HyracksDataException {
        ValueCounter item;
        for (List<ValueCounter> entry : hashes.values()) {
            for (int i = 0; i < entry.size(); i++) {
                item = entry.get(i);
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
            return true;
        } else {
            // potentially, item already exists
            ValueCounter itemListIdxCounter = ArrayFunctionsUtil.findItem(item, sameHashes);
            if (itemListIdxCounter == null) {
                // new item
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