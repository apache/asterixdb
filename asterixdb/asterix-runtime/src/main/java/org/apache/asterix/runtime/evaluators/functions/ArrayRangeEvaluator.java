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

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.typecomputer.impl.ArrayRangeTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * <pre>
 * array_range(start_num, end_num, step_num?) returns a new ordered list, list of long items or double items
 * depending on the supplied arguments. One floating-point arg will make it a list of double items. step_num is optional
 * where the default is 1. It returns an empty list for arguments like:
 * array_range(2, 20, -2), array_range(10, 3, 4) and array_range(1,6,0) where it cannot determine a proper sequence.
 *
 * It throws an error at compile time if the number of arguments < 2 or > 3
 *
 * It returns in order:
 * 1. missing, if any argument is missing.
 * 2. null, if any argument is null or they are not numeric or they are NaN +-INF.
 * 3. otherwise, a new list.
 *
 * </pre>
*/

public class ArrayRangeEvaluator extends AbstractScalarEval {
    private final OrderedListBuilder listBuilder;
    private final ArrayBackedValueStorage storage;
    private final IScalarEvaluator startNumEval;
    private final TaggedValuePointable start;
    private final IScalarEvaluator endNumEval;
    private final TaggedValuePointable end;
    private final AMutableDouble aDouble;
    private final AMutableInt64 aLong;
    private IScalarEvaluator stepNumEval;
    private TaggedValuePointable step;

    ArrayRangeEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, SourceLocation sourceLocation,
            FunctionIdentifier functionIdentifier) throws HyracksDataException {
        super(sourceLocation, functionIdentifier);
        storage = new ArrayBackedValueStorage();
        start = new TaggedValuePointable();
        end = new TaggedValuePointable();
        startNumEval = args[0].createScalarEvaluator(ctx);
        endNumEval = args[1].createScalarEvaluator(ctx);
        listBuilder = new OrderedListBuilder();
        aDouble = new AMutableDouble(0);
        aLong = new AMutableInt64(0);
        if (args.length == 3) {
            stepNumEval = args[2].createScalarEvaluator(ctx);
            step = new TaggedValuePointable();
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        startNumEval.evaluate(tuple, start);
        endNumEval.evaluate(tuple, end);
        if (stepNumEval != null) {
            stepNumEval.evaluate(tuple, step);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, start, end, step)) {
            return;
        }

        String n = functionIdentifier.getName();
        ATypeTag startTag = ATYPETAGDESERIALIZER.deserialize(start.getTag());
        ATypeTag endTag = ATYPETAGDESERIALIZER.deserialize(end.getTag());
        ATypeTag stepTag = ATypeTag.INTEGER;
        double stepNum = 1;
        if (stepNumEval != null) {
            stepNumEval.evaluate(tuple, step);
            stepTag = ATYPETAGDESERIALIZER.deserialize(step.getTag());
            if (!ATypeHierarchy.isCompatible(ATypeTag.DOUBLE, stepTag)) {
                PointableHelper.setNull(result);
                return;
            }
            stepNum = ATypeHierarchy.getDoubleValue(n, 2, step.getByteArray(), step.getStartOffset());
        }

        if (!ATypeHierarchy.isCompatible(ATypeTag.DOUBLE, startTag) || Double.isNaN(stepNum)
                || !ATypeHierarchy.isCompatible(ATypeTag.DOUBLE, endTag) || Double.isInfinite(stepNum)) {
            PointableHelper.setNull(result);
            return;
        }

        ISerializerDeserializer serde;
        if (ATypeHierarchy.canPromote(startTag, ATypeTag.BIGINT) && ATypeHierarchy.canPromote(endTag, ATypeTag.BIGINT)
                && ATypeHierarchy.canPromote(stepTag, ATypeTag.BIGINT)) {
            // all 3 numbers are whole numbers
            serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
            long startNum = ATypeHierarchy.getLongValue(n, 0, start.getByteArray(), start.getStartOffset());
            long endNum = ATypeHierarchy.getLongValue(n, 1, end.getByteArray(), end.getStartOffset());
            listBuilder.reset(ArrayRangeTypeComputer.LONG_LIST);
            while ((startNum < endNum && stepNum > 0) || (startNum > endNum && stepNum < 0)) {
                aLong.setValue(startNum);
                storage.reset();
                serde.serialize(aLong, storage.getDataOutput());
                listBuilder.addItem(storage);
                startNum += stepNum;
            }
        } else {
            // one number is a floating-point number
            serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
            double startNum = ATypeHierarchy.getDoubleValue(n, 0, start.getByteArray(), start.getStartOffset());
            double endNum = ATypeHierarchy.getDoubleValue(n, 1, end.getByteArray(), end.getStartOffset());
            if (Double.isNaN(startNum) || Double.isInfinite(startNum) || Double.isNaN(endNum)
                    || Double.isInfinite(endNum)) {
                PointableHelper.setNull(result);
                return;
            }
            listBuilder.reset(ArrayRangeTypeComputer.DOUBLE_LIST);
            while ((startNum < endNum && stepNum > 0) || (startNum > endNum && stepNum < 0)) {
                aDouble.setValue(startNum);
                storage.reset();
                serde.serialize(aDouble, storage.getDataOutput());
                listBuilder.addItem(storage);
                startNum += stepNum;
            }
        }

        storage.reset();
        listBuilder.write(storage.getDataOutput(), true);
        result.set(storage);
    }
}
