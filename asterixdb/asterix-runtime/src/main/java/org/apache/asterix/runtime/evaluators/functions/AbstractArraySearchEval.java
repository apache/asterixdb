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
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractArraySearchEval implements IScalarEvaluator {
    private final IPointable listArg;
    private final IPointable searchedValueArg;
    private final IPointable tempVal;
    private final IScalarEvaluator listEval;
    private final IScalarEvaluator searchedValueEval;
    private final IBinaryComparator comp;
    private final ListAccessor listAccessor;
    private final AMutableInt32 intValue;
    protected final ArrayBackedValueStorage storage;

    AbstractArraySearchEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, IAType[] argTypes)
            throws HyracksDataException {
        storage = new ArrayBackedValueStorage();
        listArg = new VoidPointable();
        searchedValueArg = new VoidPointable();
        tempVal = new VoidPointable();
        listEval = args[0].createScalarEvaluator(ctx);
        searchedValueEval = args[1].createScalarEvaluator(ctx);
        comp = createComparator(argTypes[0], argTypes[1]);
        listAccessor = new ListAccessor();
        intValue = new AMutableInt32(-1);
    }

    private static IBinaryComparator createComparator(IAType listType, IAType searchValueType) {
        IAType itemType = listType.getTypeTag().isListType() ? ((AbstractCollectionType) listType).getItemType()
                : BuiltinType.ANY;
        return BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(itemType, searchValueType, true)
                .createBinaryComparator();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // Evaluators
        listEval.evaluate(tuple, listArg);
        searchedValueEval.evaluate(tuple, searchedValueArg);

        if (PointableHelper.checkAndSetMissingOrNull(result, listArg, searchedValueArg)) {
            return;
        }

        // 1st arg: list
        byte[] listBytes = listArg.getByteArray();
        int listOffset = listArg.getStartOffset();

        // TODO(ali): could be optimized to not evaluate again if the search value evaluator is a constant
        // 2nd arg: value to search for
        byte[] valueBytes = searchedValueArg.getByteArray();
        int valueOffset = searchedValueArg.getStartOffset();
        int valueLength = searchedValueArg.getLength();

        if (!ATYPETAGDESERIALIZER.deserialize(listBytes[listOffset]).isListType()) {
            PointableHelper.setNull(result);
            return;
        }

        // initialize variables; -1 = value not found
        intValue.setValue(-1);
        listAccessor.reset(listBytes, listOffset);
        int numItems = listAccessor.size();
        try {
            for (int i = 0; i < numItems; i++) {
                listAccessor.getOrWriteItem(i, tempVal, storage);
                if (comp.compare(tempVal.getByteArray(), tempVal.getStartOffset(), tempVal.getLength(), valueBytes,
                        valueOffset, valueLength) == 0) {
                    intValue.setValue(i);
                    break;
                }
            }
            processResult(intValue, result);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected abstract void processResult(AMutableInt32 intValue, IPointable result) throws HyracksDataException;
}
