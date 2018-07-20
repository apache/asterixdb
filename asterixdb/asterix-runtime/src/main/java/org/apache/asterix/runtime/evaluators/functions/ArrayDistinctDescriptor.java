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

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.IOException;
import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.ArrayFunctionsUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class ArrayDistinctDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType inputListType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayDistinctDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            // the type of the input list is needed in order to use the same type for the new returned list
            return FunctionTypeInferers.SET_ARGUMENT_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_DISTINCT;
    }

    @Override
    public void setImmutableStates(Object... states) {
        inputListType = (IAType) states[0];
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new ArrayDistinctFunction(args, ctx, sourceLoc);
            }
        };
    }

    public class ArrayDistinctFunction extends AbstractArrayProcessEval {
        private final SourceLocation sourceLoc;
        private final IBinaryHashFunction binaryHashFunction;
        private final Int2ObjectMap<List<IPointable>> hashes;
        private IPointable item;
        private ArrayBackedValueStorage storage;

        public ArrayDistinctFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx, SourceLocation sourceLoc)
                throws HyracksDataException {
            super(args, ctx, inputListType);
            this.sourceLoc = sourceLoc;
            hashes = new Int2ObjectOpenHashMap<>();
            item = pointableAllocator.allocateEmpty();
            storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
            binaryHashFunction = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(null)
                    .createBinaryHashFunction();
        }

        @Override
        protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder) throws IOException {
            int hash;
            boolean itemInStorage;
            boolean nullMissingWasAdded = false;
            List<IPointable> sameHashes;
            hashes.clear();
            for (int i = 0; i < listAccessor.size(); i++) {
                // get the item and compute its hash
                itemInStorage = listAccessor.getOrWriteItem(i, item, storage);
                if (ATYPETAGDESERIALIZER.deserialize(item.getByteArray()[item.getStartOffset()]).isDerivedType()) {
                    throw new RuntimeDataException(ErrorCode.CANNOT_COMPARE_COMPLEX, sourceLoc);
                }
                if (isNullOrMissing(item)) {
                    if (!nullMissingWasAdded) {
                        listBuilder.addItem(item);
                        nullMissingWasAdded = true;
                    }
                } else {
                    // look up if it already exists
                    hash = binaryHashFunction.hash(item.getByteArray(), item.getStartOffset(), item.getLength());
                    hashes.get(hash);
                    sameHashes = hashes.get(hash);
                    if (sameHashes == null) {
                        // new item
                        sameHashes = arrayListAllocator.allocate(null);
                        sameHashes.clear();
                        addItem(item, listBuilder, itemInStorage, sameHashes);
                        hashes.put(hash, sameHashes);
                        item = pointableAllocator.allocateEmpty();
                    } else if (ArrayFunctionsUtil.findItem(item, sameHashes) == null) {
                        // new item, it could happen that two hashes are the same but they are for different items
                        addItem(item, listBuilder, itemInStorage, sameHashes);
                        item = pointableAllocator.allocateEmpty();
                    }
                }
            }
        }

        private boolean isNullOrMissing(IPointable item) {
            byte tag = item.getByteArray()[item.getStartOffset()];
            return tag == ATypeTag.SERIALIZED_NULL_TYPE_TAG || tag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG;
        }

        private void addItem(IPointable item, IAsterixListBuilder listBuilder, boolean itemInStorage,
                List<IPointable> sameHashes) throws HyracksDataException {
            sameHashes.add(item);
            listBuilder.addItem(item);
            if (itemInStorage) {
                // create new storage since the added item is using it now
                storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
            }
        }
    }
}
