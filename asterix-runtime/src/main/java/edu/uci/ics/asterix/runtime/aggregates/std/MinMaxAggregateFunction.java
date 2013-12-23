/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.hierachy.ATypeHierarchy;
import edu.uci.ics.asterix.om.types.hierachy.ITypePromoteComputer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class MinMaxAggregateFunction implements ICopyAggregateFunction {
    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage tempValForCasting = new ArrayBackedValueStorage();
    private DataOutput out;
    private ICopyEvaluator eval;
    private ATypeTag aggType;
    private IBinaryComparator cmp;
    private ITypePromoteComputer tpc;
    private final boolean isMin;
    private final boolean isLocalAgg;

    public MinMaxAggregateFunction(ICopyEvaluatorFactory[] args, IDataOutputProvider provider, boolean isMin,
            boolean isLocalAgg) throws AlgebricksException {
        out = provider.getDataOutput();
        eval = args[0].createEvaluator(inputVal);
        this.isMin = isMin;
        this.isLocalAgg = isLocalAgg;
    }

    @Override
    public void init() {
        aggType = ATypeTag.SYSTEM_NULL;
        outputVal.reset();
        tempValForCasting.reset();
    }

    @Override
    public void step(IFrameTupleReference tuple) throws AlgebricksException {
        inputVal.reset();
        eval.evaluate(tuple);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[0]);
        if (typeTag == ATypeTag.NULL || aggType == ATypeTag.NULL) {
            aggType = ATypeTag.NULL;
            return;
        }
        if (aggType == ATypeTag.SYSTEM_NULL) {
            if (typeTag == ATypeTag.SYSTEM_NULL) {
                // Ignore.
                return;
            }
            // First value encountered. Set type, comparator, and initial value.
            aggType = typeTag;
            // Set comparator.
            IBinaryComparatorFactory cmpFactory = AqlBinaryComparatorFactoryProvider.INSTANCE
                    .getBinaryComparatorFactory(aggType, isMin);
            cmp = cmpFactory.createBinaryComparator();
            // Initialize min value.
            outputVal.assign(inputVal);
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            throw new AlgebricksException("Unexpected type " + typeTag + " in aggregation input stream. Expected type "
                    + aggType + ".");
        } else {

            // If a system_null is encountered locally, it would be an error; otherwise if it is seen
            // by a global aggregator, it is simple ignored.
            if (typeTag == ATypeTag.SYSTEM_NULL) {
                if (isLocalAgg) {
                    throw new AlgebricksException("Type SYSTEM_NULL encountered in local aggregate.");
                } else {
                    return;
                }
            }

            if (ATypeHierarchy.canPromote(aggType, typeTag)) {
                tpc = ATypeHierarchy.getTypePromoteComputer(aggType, typeTag);
                aggType = typeTag;
                cmp = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(aggType, isMin)
                        .createBinaryComparator();
                if (tpc != null) {
                    tempValForCasting.reset();
                    try {
                        tpc.promote(outputVal.getByteArray(), outputVal.getStartOffset() + 1,
                                outputVal.getLength() - 1, tempValForCasting);
                    } catch (IOException e) {
                        throw new AlgebricksException(e);
                    }
                    outputVal.reset();
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
                        tpc.promote(inputVal.getByteArray(), inputVal.getStartOffset() + 1, inputVal.getLength() - 1,
                                tempValForCasting);
                    } catch (IOException e) {
                        throw new AlgebricksException(e);
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
    public void finish() throws AlgebricksException {
        try {
            switch (aggType) {
                case NULL: {
                    out.writeByte(ATypeTag.NULL.serialize());
                    break;
                }
                case SYSTEM_NULL: {
                    // Empty stream. For local agg return system null. For global agg return null.
                    if (isLocalAgg) {
                        out.writeByte(ATypeTag.SYSTEM_NULL.serialize());
                    } else {
                        out.writeByte(ATypeTag.NULL.serialize());
                    }
                    break;
                }
                default: {
                    out.write(outputVal.getByteArray(), outputVal.getStartOffset(), outputVal.getLength());
                    break;
                }
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finishPartial() throws AlgebricksException {
        finish();
    }
}
