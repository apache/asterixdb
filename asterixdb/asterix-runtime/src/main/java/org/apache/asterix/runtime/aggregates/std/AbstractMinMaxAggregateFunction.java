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
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ITypeConvertComputer;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractMinMaxAggregateFunction implements IAggregateEvaluator {
    private IPointable inputVal = new VoidPointable();
    private ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage tempValForCasting = new ArrayBackedValueStorage();

    protected ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private IScalarEvaluator eval;
    protected ATypeTag aggType;
    private IBinaryComparator cmp;
    private ITypeConvertComputer tpc;
    private final boolean isMin;

    public AbstractMinMaxAggregateFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext context, boolean isMin)
            throws HyracksDataException {
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
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
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
            IBinaryComparatorFactory cmpFactory = BinaryComparatorFactoryProvider.INSTANCE
                    .getBinaryComparatorFactory(aggType, isMin);
            cmp = cmpFactory.createBinaryComparator();
            // Initialize min value.
            outputVal.assign(inputVal);
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            throw new IncompatibleTypeException("min/max", typeTag.serialize(), aggType.serialize());
        } else {

            // If a system_null is encountered locally, it would be an error; otherwise if it is seen
            // by a global aggregator, it is simple ignored.
            if (typeTag == ATypeTag.SYSTEM_NULL) {
                processSystemNull();
                return;
            }

            if (ATypeHierarchy.canPromote(aggType, typeTag)) {
                tpc = ATypeHierarchy.getTypePromoteComputer(aggType, typeTag);
                aggType = typeTag;
                cmp = BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(aggType, isMin)
                        .createBinaryComparator();
                if (tpc != null) {
                    tempValForCasting.reset();
                    try {
                        tpc.convertType(outputVal.getByteArray(), outputVal.getStartOffset() + 1,
                                outputVal.getLength() - 1, tempValForCasting.getDataOutput());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    outputVal.assign(tempValForCasting);
                }
                if (cmp.compare(inputVal.getByteArray(), inputVal.getStartOffset(), inputVal.getLength(),
                        outputVal.getByteArray(), outputVal.getStartOffset(), outputVal.getLength()) < 0) {
                    outputVal.assign(inputVal);
                }

            } else {
                tpc = ATypeHierarchy.getTypePromoteComputer(typeTag, aggType);
                if (tpc != null) {
                    tempValForCasting.reset();
                    try {
                        tpc.convertType(inputVal.getByteArray(), inputVal.getStartOffset() + 1,
                                inputVal.getLength() - 1, tempValForCasting.getDataOutput());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    if (cmp.compare(tempValForCasting.getByteArray(), tempValForCasting.getStartOffset(),
                            tempValForCasting.getLength(), outputVal.getByteArray(), outputVal.getStartOffset(),
                            outputVal.getLength()) < 0) {
                        outputVal.assign(tempValForCasting);
                    }
                } else {
                    if (cmp.compare(inputVal.getByteArray(), inputVal.getStartOffset(), inputVal.getLength(),
                            outputVal.getByteArray(), outputVal.getStartOffset(), outputVal.getLength()) < 0) {
                        outputVal.assign(inputVal);
                    }
                }

            }
        }
    }

    @Override
    public void finish(IPointable result) throws HyracksDataException {
        resultStorage.reset();
        try {
            switch (aggType) {
                case NULL: {
                    resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                    result.set(resultStorage);
                    break;
                }
                case SYSTEM_NULL: {
                    finishSystemNull();
                    result.set(resultStorage);
                    break;
                }
                default: {
                    result.set(outputVal);
                    break;
                }
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
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
}
