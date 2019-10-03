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
package org.apache.asterix.runtime.aggregates.std;

import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import java.io.IOException;

import org.apache.asterix.dataflow.data.common.ILogicalBinaryComparator;
import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.dataflow.data.nontagged.comparators.ComparatorUtil;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ITypeConvertComputer;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractMinMaxAggregateFunction extends AbstractAggregateFunction {
    private static final String FUN_NAME = "min/max";
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final IPointable inputVal = new VoidPointable();
    private final ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage tempValForCasting = new ArrayBackedValueStorage();
    private final TaggedValueReference value1 = new TaggedValueReference();
    private final TaggedValueReference value2 = new TaggedValueReference();
    private final IScalarEvaluator eval;
    private final boolean isMin;
    private final IAType aggFieldType;
    protected final Type type;
    protected final IEvaluatorContext context;
    protected ATypeTag aggType;
    private ILogicalBinaryComparator cmp;

    AbstractMinMaxAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context, boolean isMin,
            SourceLocation sourceLoc, Type type, IAType aggFieldType) throws HyracksDataException {
        super(sourceLoc);
        this.context = context;
        this.eval = args[0].createScalarEvaluator(context);
        this.isMin = isMin;
        this.aggFieldType = aggFieldType;
        this.type = type;
    }

    @Override
    public void init() throws HyracksDataException {
        aggType = ATypeTag.SYSTEM_NULL;
        tempValForCasting.reset();
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        if (skipStep()) {
            return;
        }
        eval.evaluate(tuple, inputVal);
        ATypeTag typeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
        if (typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL) {
            processNull();
        } else if (typeTag == ATypeTag.SYSTEM_NULL) {
            // if a system_null is encountered locally, it would be an error; otherwise it is ignored
            if (type == Type.LOCAL) {
                throw new UnsupportedItemTypeException(sourceLoc, FUN_NAME, ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
            }
        } else if (aggType == ATypeTag.SYSTEM_NULL) {
            // First value encountered. Set type, comparator, and initial value.
            if (ILogicalBinaryComparator.inequalityUndefined(typeTag)) {
                handleUnsupportedInput(typeTag);
                return;
            }
            aggType = typeTag;
            cmp = ComparatorUtil.createLogicalComparator(aggFieldType, aggFieldType, false);
            outputVal.assign(inputVal);
        } else if (!ATypeHierarchy.isCompatible(typeTag, aggType)) {
            handleIncompatibleInput(typeTag);
        } else {
            // the two values are compatible non-null/non-missing values
            if (aggType == typeTag) {
                compareAndUpdate(cmp, inputVal, outputVal, typeTag);
                return;
            }
            if (ATypeHierarchy.canPromote(aggType, typeTag)) {
                // switch to new comp & aggregation type (i.e. current min/max is int and new input is double)
                castValue(ATypeHierarchy.getTypePromoteComputer(aggType, typeTag), outputVal, tempValForCasting);
                outputVal.assign(tempValForCasting);
                compareAndUpdate(cmp, inputVal, outputVal, typeTag);
                aggType = typeTag;
            } else {
                castValue(ATypeHierarchy.getTypePromoteComputer(typeTag, aggType), inputVal, tempValForCasting);
                compareAndUpdate(cmp, tempValForCasting, outputVal, typeTag);
            }
        }
    }

    @Override
    public void finish(IPointable result) throws HyracksDataException {
        finish(result, false);
    }

    @Override
    public void finishPartial(IPointable result) throws HyracksDataException {
        finish(result, true);
    }

    private void finish(IPointable result, boolean isPartial) throws HyracksDataException {
        resultStorage.reset();
        try {
            switch (aggType) {
                case NULL:
                    resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                    result.set(resultStorage);
                    break;
                case SYSTEM_NULL:
                    if (type == Type.LOCAL || type == Type.INTERMEDIATE || isPartial) {
                        resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                    } else {
                        resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                    }
                    result.set(resultStorage);
                    break;
                default:
                    result.set(outputVal);
                    break;
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected abstract void processNull();

    private boolean skipStep() {
        return aggType == ATypeTag.NULL;
    }

    private void handleIncompatibleInput(ATypeTag typeTag) {
        ExceptionUtil.warnIncompatibleType(context, sourceLoc, FUN_NAME, aggType, typeTag);
        this.aggType = ATypeTag.NULL;
    }

    private void handleUnsupportedInput(ATypeTag typeTag) {
        ExceptionUtil.warnUnsupportedType(context, sourceLoc, FUN_NAME, typeTag);
        this.aggType = ATypeTag.NULL;
    }

    private void compareAndUpdate(ILogicalBinaryComparator c, IPointable newVal, ArrayBackedValueStorage currentVal,
            ATypeTag typeTag) throws HyracksDataException {
        // newVal is never NULL/MISSING here. it's already checked up. current value is the first encountered non-null.
        byte[] newValByteArray = newVal.getByteArray();
        int newValStartOffset = newVal.getStartOffset();
        byte[] currentValByteArray = currentVal.getByteArray();
        int currentValStartOffset = currentVal.getStartOffset();
        value1.set(newValByteArray, newValStartOffset + 1, newVal.getLength() - 1,
                VALUE_TYPE_MAPPING[newValByteArray[newValStartOffset]]);
        value2.set(currentValByteArray, currentValStartOffset + 1, currentVal.getLength() - 1,
                VALUE_TYPE_MAPPING[newValByteArray[newValStartOffset]]);
        ILogicalBinaryComparator.Result result = c.compare(value1, value2);
        switch (result) {
            case LT:
                if (isMin) {
                    currentVal.assign(newVal);
                }
                break;
            case GT:
                if (!isMin) {
                    // update the current value with the new maximum
                    currentVal.assign(newVal);
                }
                break;
            case MISSING:
            case NULL:
                // this should never happen given that the two values were not NULL/MISSING. aggregation is made NULL.
                // it shouldn't happen for scalar values. For ARRAY values this should not happen as well.
                // if some elements are NULL/MISSING, the ARRAY comparison returns INCOMPARABLE.
                aggType = ATypeTag.NULL;
                return;
            case INCOMPARABLE:
                handleIncompatibleInput(typeTag);
                return;
            default:
                // EQ, do nothing
                break;
        }
    }

    private static void castValue(ITypeConvertComputer typeConverter, IPointable inputValue,
            ArrayBackedValueStorage tempValForCasting) throws HyracksDataException {
        tempValForCasting.reset();
        try {
            typeConverter.convertType(inputValue.getByteArray(), inputValue.getStartOffset() + 1,
                    inputValue.getLength() - 1, tempValForCasting.getDataOutput());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    enum Type {
        LOCAL,
        INTERMEDIATE,
        GLOBAL,
        ONE_STEP
    }
}
