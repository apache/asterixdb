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

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * <pre>
 * array_intersect(list1, list2, ...) returns a new list containing items that are present in all of the input
 * lists. Null and missing items are ignored. It's case-sensitive to string items.
 *
 * array_intersect([null, 2, missing], [3,missing,2,null]) will result in [2].
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
public class ArrayIntersectDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayIntersectDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENTS_TYPE;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_INTERSECT;
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
                return new ArrayIntersectEval(args, ctx);
            }
        };
    }

    protected class ValueListIndex implements IValueReference {
        private IPointable value;
        private int listIndex;

        ValueListIndex() {
        }

        protected void set(IPointable value, int listIndex) {
            this.value = value;
            this.listIndex = listIndex;
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

    protected class ValueListIndexAllocator implements IObjectFactory<ValueListIndex, ATypeTag> {

        ValueListIndexAllocator() {
        }

        @Override
        public ValueListIndex create(ATypeTag arg) {
            return new ValueListIndex();
        }
    }

    public class ArrayIntersectEval implements IScalarEvaluator {
        private final ListAccessor listAccessor;
        private final IPointable pointable;
        private final ArrayBackedValueStorage currentItemStorage;
        private final IPointable[] listsArgs;
        private final IScalarEvaluator[] listsEval;
        private final IBinaryHashFunction binaryHashFunction;
        private final Int2ObjectMap<List<ValueListIndex>> hashes;
        private final PointableAllocator pointableAllocator;
        private final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;
        private final IObjectPool<List<ValueListIndex>, ATypeTag> arrayListAllocator;
        private final IObjectPool<ValueListIndex, ATypeTag> valueListIndexAllocator;
        private final ArrayBackedValueStorage finalResult;
        private final CastTypeEvaluator caster;
        private final IBinaryComparator comp;
        private IAsterixListBuilder orderedListBuilder;
        private IAsterixListBuilder unorderedListBuilder;

        ArrayIntersectEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx) throws HyracksDataException {
            orderedListBuilder = null;
            unorderedListBuilder = null;
            pointableAllocator = new PointableAllocator();
            storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
            arrayListAllocator = new ListObjectPool<>(new ArrayListFactory<>());
            valueListIndexAllocator = new ListObjectPool<>(new ValueListIndexAllocator());
            hashes = new Int2ObjectOpenHashMap<>();
            finalResult = new ArrayBackedValueStorage();
            listAccessor = new ListAccessor();
            caster = new CastTypeEvaluator();
            // for functions that accept multiple lists arguments, they will be casted to open, hence item is ANY
            comp = BinaryComparatorFactoryProvider.INSTANCE
                    .getBinaryComparatorFactory(BuiltinType.ANY, BuiltinType.ANY, true).createBinaryComparator();
            listsArgs = new IPointable[args.length];
            listsEval = new IScalarEvaluator[args.length];
            pointable = new VoidPointable();
            currentItemStorage = new ArrayBackedValueStorage();
            for (int i = 0; i < args.length; i++) {
                listsArgs[i] = new VoidPointable();
                listsEval[i] = args[i].createScalarEvaluator(ctx);
            }
            binaryHashFunction = BinaryHashFunctionFactoryProvider.INSTANCE
                    .getBinaryHashFunctionFactory(BuiltinType.ANY).createBinaryHashFunction();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            byte listArgType;
            boolean isReturnNull = false;
            AbstractCollectionType outList = null;
            ATypeTag listTag;
            int minListIndex = 0;
            int minSize = -1;
            int nextSize;

            // evaluate all the lists first to make sure they're all actually lists and of the same list type
            try {
                for (int i = 0; i < listsEval.length; i++) {
                    listsEval[i].evaluate(tuple, pointable);

                    if (PointableHelper.checkAndSetMissingOrNull(result, pointable)) {
                        if (result.getByteArray()[0] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                            return;
                        }

                        // null value, but check other arguments for missing first (higher priority)
                        isReturnNull = true;
                    }

                    if (!isReturnNull) {
                        listArgType = pointable.getByteArray()[pointable.getStartOffset()];
                        listTag = ATYPETAGDESERIALIZER.deserialize(listArgType);
                        if (!listTag.isListType()) {
                            isReturnNull = true;
                        } else if (outList != null && outList.getTypeTag() != listTag) {
                            throw new RuntimeDataException(ErrorCode.DIFFERENT_LIST_TYPE_ARGS, sourceLoc);
                        } else {
                            if (outList == null) {
                                outList =
                                        (AbstractCollectionType) DefaultOpenFieldType.getDefaultOpenFieldType(listTag);
                            }

                            caster.resetAndAllocate(outList, argTypes[i], listsEval[i]);
                            caster.cast(pointable, listsArgs[i]);
                            nextSize = getNumItems(outList, listsArgs[i].getByteArray(), listsArgs[i].getStartOffset());
                            if (nextSize < minSize || minSize == -1) {
                                minSize = nextSize;
                                minListIndex = i;
                            }
                        }
                    }
                }

                if (isReturnNull) {
                    PointableHelper.setNull(result);
                    return;
                }

                IAsterixListBuilder listBuilder;
                if (outList.getTypeTag() == ATypeTag.ARRAY) {
                    if (orderedListBuilder == null) {
                        orderedListBuilder = new OrderedListBuilder();
                    }
                    listBuilder = orderedListBuilder;
                } else {
                    if (unorderedListBuilder == null) {
                        unorderedListBuilder = new UnorderedListBuilder();
                    }
                    listBuilder = unorderedListBuilder;
                }

                IPointable listArg;
                hashes.clear();

                // first, get distinct items of the most restrictive (smallest) list.
                // values will be added to listBuilder after inspecting all input lists
                listArg = listsArgs[minListIndex];
                listAccessor.reset(listArg.getByteArray(), listArg.getStartOffset());
                buildRestrictiveList(listAccessor);
                listBuilder.reset(outList);

                if (!hashes.isEmpty()) {
                    // process each list one by one
                    for (int listIndex = 0; listIndex < listsArgs.length; listIndex++) {
                        // TODO(ali): find a way to avoid comparing the smallest list
                        listArg = listsArgs[listIndex];
                        listAccessor.reset(listArg.getByteArray(), listArg.getStartOffset());
                        processList(listAccessor, listIndex, listBuilder);
                    }
                }

                finalResult.reset();
                listBuilder.write(finalResult.getDataOutput(), true);
                result.set(finalResult);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            } finally {
                caster.deallocatePointables();
                valueListIndexAllocator.reset();
                storageAllocator.reset();
                arrayListAllocator.reset();
                pointableAllocator.reset();
            }
        }

        private int getNumItems(AbstractCollectionType listType, byte[] listBytes, int offset) {
            if (listType.getTypeTag() == ATypeTag.ARRAY) {
                return AOrderedListSerializerDeserializer.getNumberOfItems(listBytes, offset);
            } else {
                return AUnorderedListSerializerDeserializer.getNumberOfItems(listBytes, offset);
            }
        }

        // puts all the items of the smallest list in "hashes"
        private void buildRestrictiveList(ListAccessor listAccessor) throws IOException {
            if (listAccessor.size() > 0) {
                int hash;
                List<ValueListIndex> sameHashes;
                boolean itemInStorage;
                IPointable item = pointableAllocator.allocateEmpty();
                ArrayBackedValueStorage storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                storage.reset();
                for (int j = 0; j < listAccessor.size(); j++) {
                    itemInStorage = listAccessor.getOrWriteItem(j, item, storage);
                    if (notNullAndMissing(item)) {
                        hash = binaryHashFunction.hash(item.getByteArray(), item.getStartOffset(), item.getLength());
                        sameHashes = hashes.get(hash);
                        if (addToSmallestList(item, hash, sameHashes)) {
                            // item has been added to intersect list and is being used, allocate new pointable
                            item = pointableAllocator.allocateEmpty();
                            if (itemInStorage) {
                                storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                                storage.reset();
                            }
                        }
                    }
                }
            }
        }

        private void processList(ListAccessor listAccessor, int listIndex, IAsterixListBuilder listBuilder)
                throws IOException {
            int hash;
            List<ValueListIndex> sameHashes;
            for (int j = 0; j < listAccessor.size(); j++) {
                listAccessor.getOrWriteItem(j, pointable, currentItemStorage);
                if (notNullAndMissing(pointable)) {
                    // hash the item and look up to see if it is common
                    hash = binaryHashFunction.hash(pointable.getByteArray(), pointable.getStartOffset(),
                            pointable.getLength());
                    sameHashes = hashes.get(hash);
                    incrementIfCommonValue(pointable, sameHashes, listIndex, listBuilder);
                }
            }
        }

        // collects the items of the most restrictive list, it initializes the list index as -1. each successive list
        // should stamp the value with its list index if the list has the item. It starts with list index = 0
        private boolean addToSmallestList(IPointable item, int hash, List<ValueListIndex> sameHashes)
                throws IOException {
            // add if new item
            if (sameHashes == null) {
                List<ValueListIndex> newHashes = arrayListAllocator.allocate(null);
                newHashes.clear();
                ValueListIndex valueListIndex = valueListIndexAllocator.allocate(null);
                valueListIndex.set(item, -1);
                newHashes.add(valueListIndex);
                hashes.put(hash, newHashes);
                return true;
            } else if (PointableHelper.findItem(item, sameHashes, comp) == null) {
                ValueListIndex valueListIndex = valueListIndexAllocator.allocate(null);
                valueListIndex.set(item, -1);
                sameHashes.add(valueListIndex);
                return true;
            }
            // else ignore for duplicate values in the same list
            return false;
        }

        private void incrementIfCommonValue(IPointable item, List<ValueListIndex> sameHashes, int listIndex,
                IAsterixListBuilder listBuilder) throws IOException {
            if (sameHashes != null) {
                // look for the same equal item, add to list builder when all lists have seen this item
                incrementIfExists(sameHashes, item, listIndex, listBuilder);
            }
        }

        private boolean notNullAndMissing(IPointable item) {
            byte tag = item.getByteArray()[item.getStartOffset()];
            return tag != ATypeTag.SERIALIZED_NULL_TYPE_TAG && tag != ATypeTag.SERIALIZED_MISSING_TYPE_TAG;
        }

        private void incrementIfExists(List<ValueListIndex> sameHashes, IPointable item, int listIndex,
                IAsterixListBuilder listBuilder) throws HyracksDataException {
            ValueListIndex sameValue = PointableHelper.findItem(item, sameHashes, comp);
            if (sameValue != null && listIndex - sameValue.listIndex == 1) {
                // found the item, its stamp is OK (stamp saves the index of the last list that has seen this item)
                // increment stamp of this item
                sameValue.listIndex = listIndex;
                if (listIndex == listsArgs.length - 1) {
                    // if this list is the last to stamp, then add to the final result
                    listBuilder.addItem(item);
                }
            }
        }
    }
}
