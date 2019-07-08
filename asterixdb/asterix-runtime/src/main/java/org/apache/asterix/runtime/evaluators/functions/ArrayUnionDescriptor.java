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
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * <pre>
 * array_union(list1, list2, ...) returns a new list with the set union of the input lists (no duplicates).
 * Items of the lists can be null or missing (both are added as a null value).
 * array_union([null, 2], [missing, 3, null]) will result in [null, 2, null, 3] where one null is for the missing item
 * and the second null for the null item.
 *
 * It throws an error at compile time if the number of arguments < 2
 *
 * It returns (or throws an error at runtime) in order:
 * 1. missing, if any argument is missing.
 * 2. an error if the input lists are not of the same type (one is an ordered list while the other is unordered).
 * 3. null, if any input list is null or is not a list.
 * 4. otherwise, a new list.
 *
 * </pre>
 */

@MissingNullInOutFunction
public class ArrayUnionDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayUnionDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENTS_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_UNION;
    }

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = (IAType[]) states;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayUnionEval(args, ctx);
            }
        };
    }

    public class ArrayUnionEval extends AbstractArrayProcessArraysEval {
        private final IObjectPool<List<IPointable>, ATypeTag> pointableListAllocator;
        private final IBinaryHashFunction binaryHashFunction;
        private final Int2ObjectMap<List<IPointable>> hashes;
        private final IBinaryComparator comp;

        ArrayUnionEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            super(args, ctx, sourceLoc, argTypes);
            pointableListAllocator = new ListObjectPool<>(new ArrayListFactory<>());
            hashes = new Int2ObjectOpenHashMap<>();
            // for functions that accept multiple lists arguments, they will be casted to open, hence item is ANY
            comp = BinaryComparatorFactoryProvider.INSTANCE
                    .getBinaryComparatorFactory(BuiltinType.ANY, BuiltinType.ANY, true).createBinaryComparator();
            binaryHashFunction = BinaryHashFunctionFactoryProvider.INSTANCE
                    .getBinaryHashFunctionFactory(BuiltinType.ANY).createBinaryHashFunction();
        }

        @Override
        protected void init() {
            hashes.clear();
        }

        @Override
        protected void finish(IAsterixListBuilder listBuilder) {
            // do nothing
        }

        @Override
        protected void release() {
            pointableListAllocator.reset();
        }

        @Override
        protected boolean processItem(IPointable item, int listIndex, IAsterixListBuilder listBuilder)
                throws HyracksDataException {
            int hash = binaryHashFunction.hash(item.getByteArray(), item.getStartOffset(), item.getLength());
            List<IPointable> sameHashes = hashes.get(hash);
            if (sameHashes == null) {
                // new item
                sameHashes = pointableListAllocator.allocate(null);
                sameHashes.clear();
                addItem(listBuilder, item, sameHashes);
                hashes.put(hash, sameHashes);
                return true;
            } else if (PointableHelper.findItem(item, sameHashes, comp) == null) {
                // new item, it could happen that two hashes are the same but they are for different items
                addItem(listBuilder, item, sameHashes);
                return true;
            }
            // else ignore since the item already exists
            return false;
        }

        private void addItem(IAsterixListBuilder listBuilder, IPointable item, List<IPointable> sameHashes)
                throws HyracksDataException {
            listBuilder.addItem(item);
            sameHashes.add(item);
        }
    }
}
