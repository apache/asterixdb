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

import java.io.IOException;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ITypeConvertComputer;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractMinMaxAggregateFunction extends AbstractAggregateFunction {
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final IPointable inputVal = new VoidPointable();
    private final ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
    private final ArrayBackedValueStorage tempValForCasting = new ArrayBackedValueStorage();
    private final IScalarEvaluator eval;
    private final boolean isMin;
    protected ATypeTag aggType;
    private IBinaryComparator cmp;

    AbstractMinMaxAggregateFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext context, boolean isMin,
            SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        eval = args[0].createScalarEvaluator(context);
        this.isMin = isMin;
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
            return;
        } else if (aggType == ATypeTag.SYSTEM_NULL) {
            if (typeTag == ATypeTag.SYSTEM_NULL) {
                // Ignore.
                return;
            }
            // First value encountered. Set type, comparator, and initial value.
            aggType = typeTag;
            // Set comparator.
            cmp = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(aggType, isMin)
                    .createBinaryComparator();
            // Initialize min value.
            outputVal.assign(inputVal);
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            if (typeTag.ordinal() > aggType.ordinal()) {
                throw new IncompatibleTypeException(sourceLoc, "min/max", typeTag.serialize(), aggType.serialize());
            } else {
                throw new IncompatibleTypeException(sourceLoc, "min/max", aggType.serialize(), typeTag.serialize());
            }
        } else {
            // If a system_null is encountered locally, it would be an error; otherwise if it is seen
            // by a global aggregator, it is simple ignored.
            if (typeTag == ATypeTag.SYSTEM_NULL) {
                processSystemNull();
                return;
            }
            if (aggType == typeTag) {
                compareAndUpdate(cmp, inputVal, outputVal);
                return;
            }
            if (ATypeHierarchy.canPromote(aggType, typeTag)) {
                // switch to new comp & aggregation type (i.e. current min/max is int and new input is double)
                cmp = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(typeTag, isMin)
                        .createBinaryComparator();
                castValue(ATypeHierarchy.getTypePromoteComputer(aggType, typeTag), outputVal, tempValForCasting);
                outputVal.assign(tempValForCasting);
                compareAndUpdate(cmp, inputVal, outputVal);
                aggType = typeTag;
            } else {
                castValue(ATypeHierarchy.getTypePromoteComputer(typeTag, aggType), inputVal, tempValForCasting);
                compareAndUpdate(cmp, tempValForCasting, outputVal);
            }
        }
    }

    @Override
    public void finish(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            switch (aggType) {
                case NULL:
                    resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                    result.set(resultStorage);
                    break;
                case SYSTEM_NULL:
                    finishSystemNull();
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

    @Override
    public void finishPartial(IPointable result) throws HyracksDataException {
        finish(result);
    }

    protected boolean skipStep() {
        return false;
    }

    protected abstract void processNull();

    protected abstract void processSystemNull() throws HyracksDataException;

    protected abstract void finishSystemNull() throws IOException;

    private static void compareAndUpdate(IBinaryComparator comp, IPointable newVal, ArrayBackedValueStorage oldVal)
            throws HyracksDataException {
        if (comp.compare(newVal.getByteArray(), newVal.getStartOffset(), newVal.getLength(), oldVal.getByteArray(),
                oldVal.getStartOffset(), oldVal.getLength()) < 0) {
            oldVal.assign(newVal);
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
}
