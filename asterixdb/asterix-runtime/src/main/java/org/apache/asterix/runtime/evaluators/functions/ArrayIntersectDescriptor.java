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
import java.util.Collection;
import java.util.List;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.formats.nontagged.BinaryHashFunctionFactoryProvider;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.ArrayFunctionsUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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
 * 4. an error if any list item is a list/object type (i.e. derived type) since deep equality is not yet supported.
 * 5. otherwise, a new list.
 *
 * </pre>
 */
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

    public class ValueListIndex implements IValueReference {
        private final IPointable value;
        private int listIndex;

        public ValueListIndex(IPointable value, int listIndex) {
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
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new ArrayIntersectEval(args, ctx);
            }
        };
    }

    public class ArrayIntersectEval implements IScalarEvaluator {
        private final ListAccessor listAccessor;
        private final IPointable[] listsArgs;
        private final IScalarEvaluator[] listsEval;
        private final IBinaryHashFunction binaryHashFunction;
        private final Int2ObjectMap<List<ValueListIndex>> hashes;
        private final PointableAllocator pointableAllocator;
        private final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;
        private final IObjectPool<List<ValueListIndex>, ATypeTag> arrayListAllocator;
        private final ArrayBackedValueStorage finalResult;
        private final CastTypeEvaluator caster;
        private final IBinaryComparator comp;
        private IAsterixListBuilder orderedListBuilder;
        private IAsterixListBuilder unorderedListBuilder;

        public ArrayIntersectEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx) throws HyracksDataException {
            orderedListBuilder = null;
            unorderedListBuilder = null;
            pointableAllocator = new PointableAllocator();
            storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
            arrayListAllocator = new ListObjectPool<>(new ArrayListFactory<>());
            hashes = new Int2ObjectOpenHashMap<>();
            finalResult = new ArrayBackedValueStorage();
            listAccessor = new ListAccessor();
            caster = new CastTypeEvaluator();
            comp = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();
            listsArgs = new IPointable[args.length];
            listsEval = new IScalarEvaluator[args.length];
            for (int i = 0; i < args.length; i++) {
                listsArgs[i] = new VoidPointable();
                listsEval[i] = args[i].createScalarEvaluator(ctx);
            }
            binaryHashFunction = BinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(null)
                    .createBinaryHashFunction();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            byte listArgType;
            boolean returnNull = false;
            AbstractCollectionType outList = null;
            ATypeTag listTag;
            int minListIndex = 0;
            int minSize = -1;
            int nextSize;
            IScalarEvaluator listEval;
            IPointable listArg;

            // evaluate all the lists first to make sure they're all actually lists and of the same list type
            for (int i = 0; i < listsEval.length; i++) {
                listEval = listsEval[i];
                listEval.evaluate(tuple, listsArgs[i]);
                if (!returnNull) {
                    listArg = listsArgs[i];
                    listArgType = listArg.getByteArray()[listArg.getStartOffset()];
                    listTag = ATYPETAGDESERIALIZER.deserialize(listArgType);
                    if (!listTag.isListType()) {
                        returnNull = true;
                    } else if (outList != null && outList.getTypeTag() != listTag) {
                        throw new RuntimeDataException(ErrorCode.DIFFERENT_LIST_TYPE_ARGS, sourceLoc);
                    } else {
                        if (outList == null) {
                            outList = (AbstractCollectionType) DefaultOpenFieldType.getDefaultOpenFieldType(listTag);
                        }

                        caster.reset(outList, argTypes[i], listsEval[i]);
                        caster.evaluate(tuple, listsArgs[i]);
                        nextSize = getNumItems(outList, listArg.getByteArray(), listArg.getStartOffset());
                        if (nextSize < minSize) {
                            minSize = nextSize;
                            minListIndex = i;
                        }
                    }
                }
            }

            if (returnNull) {
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

            hashes.clear();
            try {
                // first, get distinct items of the most restrictive (smallest) list, pass listBuilder as null since
                // we're not adding values yet. Values will be added to listBuilder after inspecting all input lists
                listArg = listsArgs[minListIndex];
                listAccessor.reset(listArg.getByteArray(), listArg.getStartOffset());
                processList(listAccessor, minListIndex, null, true);

                // now process each list one by one
                listBuilder.reset(outList);
                for (int listIndex = 0; listIndex < listsArgs.length; listIndex++) {
                    if (listIndex == minListIndex) {
                        incrementSmallest(listIndex, hashes.values());
                    } else {
                        listArg = listsArgs[listIndex];
                        listAccessor.reset(listArg.getByteArray(), listArg.getStartOffset());
                        processList(listAccessor, listIndex, listBuilder, false);
                    }
                }

                finalResult.reset();
                listBuilder.write(finalResult.getDataOutput(), true);
                result.set(finalResult);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            } finally {
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

        private void processList(ListAccessor listAccessor, int listIndex, IAsterixListBuilder listBuilder,
                boolean initIntersectList) throws IOException {
            int hash;
            List<ValueListIndex> sameHashes;
            boolean itemInStorage;
            IPointable item = pointableAllocator.allocateEmpty();
            ArrayBackedValueStorage storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
            storage.reset();
            for (int j = 0; j < listAccessor.size(); j++) {
                itemInStorage = listAccessor.getOrWriteItem(j, item, storage);
                if (ATYPETAGDESERIALIZER.deserialize(item.getByteArray()[item.getStartOffset()]).isDerivedType()) {
                    throw new RuntimeDataException(ErrorCode.CANNOT_COMPARE_COMPLEX, sourceLoc);
                }
                if (notNullAndMissing(item)) {
                    // look up to see if item exists
                    hash = binaryHashFunction.hash(item.getByteArray(), item.getStartOffset(), item.getLength());
                    sameHashes = hashes.get(hash);
                    if (initIntersectList && initIntersectList(item, hash, sameHashes)) {
                        // item is used
                        item = pointableAllocator.allocateEmpty();
                        if (itemInStorage) {
                            storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                            storage.reset();
                        }
                    } else {
                        incrementCommonValue(item, sameHashes, listIndex, listBuilder);
                    }
                }
            }
        }

        // collect the items of the most restrictive list, it initializes the list index as -1. each successive list
        // should stamp the value with its list index if the list has the item. It starts with list index = 0
        private boolean initIntersectList(IPointable item, int hash, List<ValueListIndex> sameHashes)
                throws IOException {
            // add if new item
            if (sameHashes == null) {
                List<ValueListIndex> newHashes = arrayListAllocator.allocate(null);
                newHashes.clear();
                newHashes.add(new ValueListIndex(item, -1));
                hashes.put(hash, newHashes);
                return true;
            } else if (ArrayFunctionsUtil.findItem(item, sameHashes, comp) == null) {
                sameHashes.add(new ValueListIndex(item, -1));
                return true;
            }
            // else ignore for duplicate values in the same list
            return false;
        }

        private void incrementCommonValue(IPointable item, List<ValueListIndex> sameHashes, int listIndex,
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

        // this method is only for the most restrictive list. it avoids comparison since it is the initial list we start
        // with, so for sure every element in the collection must exist in the list
        private void incrementSmallest(int listIndex, Collection<List<ValueListIndex>> commonValues) {
            for (List<ValueListIndex> items : commonValues) {
                for (int i = 0; i < items.size(); i++) {
                    // any difference that is not == 1 means either this current list has already stamped and advanced
                    // the stamp or the item is not common among lists because if it's common then each list should've
                    // incremented the item list index up to the current list index
                    if (listIndex - items.get(i).listIndex == 1) {
                        items.get(i).listIndex = listIndex;
                    }
                }
            }
        }

        private void incrementIfExists(List<ValueListIndex> sameHashes, IPointable item, int listIndex,
                IAsterixListBuilder listBuilder) throws HyracksDataException {
            ValueListIndex sameValue = ArrayFunctionsUtil.findItem(item, sameHashes, comp);
            if (sameValue != null && listIndex - sameValue.listIndex == 1) {
                // found the item, its stamp is OK (stamp saves the last list index that has seen this item)
                // increment stamp of this item
                sameValue.listIndex = listIndex;
                if (listIndex == listsArgs.length - 1) {
                    // when listIndex is the last list, then it means this item was found in all previous lists
                    listBuilder.addItem(item);
                }
            }
        }
    }
}
