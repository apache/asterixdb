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

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.asterix.runtime.utils.DescriptorFactoryUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * array_binary_search(orderedList, searchValue) returns the index of the search value if it exists within the
 * ordered list.
 *
 * It returns in order:
 * Missing, if any of the arguments are missing.
 * Null, if the arguments are null, if the list argument is not a list, or if the searchValue argument is not numerical.
 * Otherwise, it returns the index of the first occurrence of the search value in the input list.
 */

public class ArrayBinarySearchDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private IAType[] argTypes;

    public static final IFunctionDescriptorFactory FACTORY = DescriptorFactoryUtil
            .createFactory(ArrayBinarySearchDescriptor::new, FunctionTypeInferers.SET_ARGUMENTS_TYPE);

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = (IAType[]) states;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_BINARY_SEARCH;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new ArrayBinarySearchDescriptor.ArrayBinarySearchEval(args, ctx, argTypes);
            }
        };
    }

    public class ArrayBinarySearchEval implements IScalarEvaluator {

        private final ArrayBackedValueStorage storage;
        private final ArrayBackedValueStorage tempStorage;
        private final IScalarEvaluator listArgEval;
        private final IScalarEvaluator searchArgEval;
        private final IPointable listArg;
        private final IPointable searchArg;
        private final IPointable tempVal;
        private final IPointable tempVal2;
        private final ListAccessor listAccessor;
        private final IBinaryComparator comp;
        private final ISerializerDeserializer<AInt32> serde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
        private final AMutableInt32 resIndex = new AMutableInt32(0);

        public ArrayBinarySearchEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, IAType[] argTypes)
                throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            tempStorage = new ArrayBackedValueStorage();
            listArg = new VoidPointable();
            searchArg = new VoidPointable();
            tempVal = new VoidPointable();
            tempVal2 = new VoidPointable();
            listArgEval = args[0].createScalarEvaluator(ctx);
            searchArgEval = args[1].createScalarEvaluator(ctx);
            listAccessor = new ListAccessor();
            comp = createComparator(argTypes[0], argTypes[1]);
        }

        private IBinaryComparator createComparator(IAType listType, IAType searchValueType) {
            IAType itemType = listType.getTypeTag().isListType() ? ((AbstractCollectionType) listType).getItemType()
                    : BuiltinType.ANY;
            return BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(itemType, searchValueType, true)
                    .createBinaryComparator();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {

            // argument missing/null checks
            listArgEval.evaluate(tuple, listArg);
            searchArgEval.evaluate(tuple, searchArg);
            if (PointableHelper.checkAndSetMissingOrNull(result, listArg, searchArg)) {
                return;
            }

            // Checking that our list arg is in fact a list
            byte[] listBytes = listArg.getByteArray();
            int offset = listArg.getStartOffset();
            ATypeTag listType = ATYPETAGDESERIALIZER.deserialize(listBytes[offset]);

            if (listType != ATypeTag.ARRAY) {
                PointableHelper.setNull(result);
                return;
            }

            byte[] searchBytes = searchArg.getByteArray();
            int searchOffset = searchArg.getStartOffset();

            listAccessor.reset(listBytes, offset);

            int listLen = listAccessor.size();
            int low = 0;
            int high = listLen - 1;

            try {
                while (low <= high) {
                    int mid = low + ((high - low) / 2);
                    storage.reset();
                    listAccessor.getOrWriteItem(mid, tempVal, storage);
                    int comparison = comp.compare(tempVal.getByteArray(), tempVal.getStartOffset(), tempVal.getLength(),
                            searchBytes, searchOffset, searchArg.getLength());
                    if (comparison == 0) {
                        // if found, then find the first occurrence of the searchValue (from left to right)
                        int firstFoundIndex =
                                fetchFirstValue(mid, storage, tempStorage, listAccessor, comp, tempVal, tempVal2);
                        storage.reset();
                        resIndex.setValue(firstFoundIndex);
                        serde.serialize(resIndex, storage.getDataOutput());
                        result.set(storage);
                        return;
                    } else if (comparison < 0) {
                        low = mid + 1;
                    } else {
                        high = mid - 1;
                    }
                }
                storage.reset();
                resIndex.setValue(-1);
                serde.serialize(resIndex, storage.getDataOutput());
                result.set(storage);
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    private int fetchFirstValue(int midIndexArg, ArrayBackedValueStorage storage, ArrayBackedValueStorage storage2,
            ListAccessor listAccessor, IBinaryComparator comp, IPointable tempVal1, IPointable tempVal2)
            throws IOException {

        int midIndex = midIndexArg;

        if (midIndex == 0) {
            return midIndex;
        }
        storage.reset();
        listAccessor.getOrWriteItem(midIndex, tempVal1, storage);
        storage.reset();
        listAccessor.getOrWriteItem(midIndex - 1, tempVal2, storage2);
        int prevComparison = comp.compare(tempVal1.getByteArray(), tempVal1.getStartOffset(), tempVal1.getLength(),
                tempVal2.getByteArray(), tempVal2.getStartOffset(), tempVal2.getLength());
        // If values before current value are not equal, then return current position.
        if (prevComparison != 0) {
            return midIndex;
        } else {
            // midIndex-1 position was already checked, so we now start checking the previous positions
            midIndex--;
            // to count the number of positions before the "midIndex" value to find first occurrence of search value.
            int counter = 0;
            while (prevComparison == 0) {
                counter++;
                if (midIndex - counter == 0) {
                    return 0;
                }
                storage2.reset();
                listAccessor.getOrWriteItem(midIndex - counter, tempVal2, storage2);
                prevComparison = comp.compare(tempVal1.getByteArray(), tempVal1.getStartOffset(), tempVal1.getLength(),
                        tempVal2.getByteArray(), tempVal2.getStartOffset(), tempVal2.getLength());
                if (prevComparison != 0) {
                    return (midIndex - counter + 1);
                }
            }
        }
        return -1;
    }
}
