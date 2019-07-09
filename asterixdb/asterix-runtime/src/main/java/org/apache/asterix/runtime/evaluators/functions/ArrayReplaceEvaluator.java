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

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
* <pre>
* array_replace(list, val1, val2, max_num_times?) returns a new list with the occurrences of val1 replaced with
* val2. max_num_times arg is optional. If supplied, it replaces val1 as many as max_num_times. Any negative number for
* max_num_times means "replace all occurrences". val2 can be null meaning you can replace existing items with nulls.
*
* array_replace([2,3,3,3,1], 3, 8, 0) will do nothing and result in [2,3,3,3,1].
*
* It throws an error at compile time if the number of arguments < 3 or > 4
*
* It returns (or throws an error at runtime) in order:
* 1. missing, if any argument is missing.
* 2. null, if:
* - any argument is null (except for val2).
* - input list is not a list.
* - num_times is not numeric or it's a floating-point number with decimals, e.g, 3.2 (3.0 is OK).
* 3. otherwise, a new list.
*
* </pre>
*/

public class ArrayReplaceEvaluator extends AbstractScalarEval {

    private final IAType inputListType;
    private final IAType newValueType;
    private final IScalarEvaluator listEval;
    private final IScalarEvaluator targetValEval;
    private final IScalarEvaluator newValEval;
    private IScalarEvaluator maxEval;
    private final IPointable list;
    private final IPointable tempList;
    private final IPointable target;
    private final IPointable newVal;
    private final IPointable tempVal;
    private TaggedValuePointable maxArg;
    private final AbstractPointable item;
    private final ListAccessor listAccessor;
    private final IBinaryComparator comp;
    private final ArrayBackedValueStorage storage;
    private final CastTypeEvaluator caster;
    private IAsterixListBuilder orderedListBuilder;
    private IAsterixListBuilder unorderedListBuilder;

    ArrayReplaceEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, SourceLocation sourceLocation,
            FunctionIdentifier functionIdentifier, IAType[] argTypes) throws HyracksDataException {
        super(sourceLocation, functionIdentifier);
        storage = new ArrayBackedValueStorage();
        listEval = args[0].createScalarEvaluator(ctx);
        targetValEval = args[1].createScalarEvaluator(ctx);
        newValEval = args[2].createScalarEvaluator(ctx);
        if (args.length == 4) {
            maxEval = args[3].createScalarEvaluator(ctx);
            maxArg = new TaggedValuePointable();
        }
        list = new VoidPointable();
        tempList = new VoidPointable();
        target = new VoidPointable();
        newVal = new VoidPointable();
        tempVal = new VoidPointable();
        item = new VoidPointable();
        listAccessor = new ListAccessor();
        caster = new CastTypeEvaluator();
        orderedListBuilder = null;
        unorderedListBuilder = null;

        inputListType = argTypes[0];
        IAType targetValueType = argTypes[1];
        newValueType = argTypes[2];

        // the input list will be opened, therefore, the type of left (item type) is ANY
        comp = BinaryComparatorFactoryProvider.INSTANCE
                .getBinaryComparatorFactory(BuiltinType.ANY, targetValueType, true).createBinaryComparator();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        storage.reset();
        listEval.evaluate(tuple, tempList);
        targetValEval.evaluate(tuple, target);
        newValEval.evaluate(tuple, tempVal);
        ATypeTag listType = ATYPETAGDESERIALIZER.deserialize(tempList.getByteArray()[tempList.getStartOffset()]);
        ATypeTag targetTag = ATYPETAGDESERIALIZER.deserialize(target.getByteArray()[target.getStartOffset()]);
        ATypeTag newValTag = ATYPETAGDESERIALIZER.deserialize(tempVal.getByteArray()[tempVal.getStartOffset()]);
        if (listType == ATypeTag.MISSING || targetTag == ATypeTag.MISSING || newValTag == ATypeTag.MISSING) {
            PointableHelper.setMissing(result);
            return;
        }

        double maxDouble = -1;
        String name = functionIdentifier.getName();
        if (maxEval != null) {
            maxEval.evaluate(tuple, maxArg);
            ATypeTag maxTag = ATYPETAGDESERIALIZER.deserialize(maxArg.getTag());
            if (maxTag == ATypeTag.MISSING) {
                PointableHelper.setMissing(result);
                return;
            } else if (!ATypeHierarchy.isCompatible(maxTag, ATypeTag.DOUBLE)) {
                PointableHelper.setNull(result);
                return;
            }
            maxDouble = ATypeHierarchy.getDoubleValue(name, 3, maxArg.getByteArray(), maxArg.getStartOffset());
        }

        if (!listType.isListType() || Math.floor(maxDouble) < maxDouble || targetTag == ATypeTag.NULL
                || Double.isInfinite(maxDouble) || Double.isNaN(maxDouble)) {
            PointableHelper.setNull(result);
            return;
        }
        try {
            IAType defaultOpenType = DefaultOpenFieldType.getDefaultOpenFieldType(listType);
            caster.resetAndAllocate(defaultOpenType, inputListType, listEval);
            caster.cast(tempList, list);
            defaultOpenType = DefaultOpenFieldType.getDefaultOpenFieldType(newValTag);
            if (defaultOpenType != null) {
                caster.resetAndAllocate(defaultOpenType, newValueType, newValEval);
                caster.cast(tempVal, newVal);
            } else {
                newVal.set(tempVal);
            }
            int max = (int) maxDouble;
            // create list
            IAsterixListBuilder listBuilder;
            if (listType == ATypeTag.ARRAY) {
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
            listBuilder.reset((AbstractCollectionType) DefaultOpenFieldType.getDefaultOpenFieldType(listType));
            listAccessor.reset(list.getByteArray(), list.getStartOffset());
            int counter = 0;
            byte[] targetBytes = target.getByteArray();
            int offset = target.getStartOffset();
            int length = target.getLength();
            for (int i = 0; i < listAccessor.size(); i++) {
                listAccessor.getOrWriteItem(i, item, storage);
                if (counter != max && comp.compare(item.getByteArray(), item.getStartOffset(), item.getLength(),
                        targetBytes, offset, length) == 0) {
                    listBuilder.addItem(newVal);
                    counter++;
                } else {
                    listBuilder.addItem(item);
                }
            }
            storage.reset();
            listBuilder.write(storage.getDataOutput(), true);
            result.set(storage);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            caster.deallocatePointables();
        }
    }
}
