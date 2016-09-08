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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.base.temporal.DurationArithmeticOperations;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.typecomputer.impl.ADateTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class OverlapBinsDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new OverlapBinsDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();

                    private final IPointable argPtr0 = new VoidPointable();
                    private final IPointable argPtr1 = new VoidPointable();
                    private final IPointable argPtr2 = new VoidPointable();

                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval2 = args[2].createScalarEvaluator(ctx);

                    // for output
                    private OrderedListBuilder listBuilder = new OrderedListBuilder();
                    private ArrayBackedValueStorage listStorage = new ArrayBackedValueStorage();
                    protected final AOrderedListType intListType = new AOrderedListType(BuiltinType.AINTERVAL, null);

                    private final AMutableInterval aInterval = new AMutableInterval(0, 0, (byte) -1);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInterval> intervalSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINTERVAL);

                    private final GregorianCalendarSystem gregCalSys = GregorianCalendarSystem.getInstance();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);
                        eval2.evaluate(tuple, argPtr2);

                        byte[] bytes0 = argPtr0.getByteArray();
                        int offset0 = argPtr0.getStartOffset();

                        ATypeTag type0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]);

                        long intervalStart;
                        long intervalEnd;
                        byte intervalTypeTag;

                        if (type0 == ATypeTag.INTERVAL) {
                            intervalStart = AIntervalSerializerDeserializer.getIntervalStart(bytes0, offset0 + 1);
                            intervalEnd = AIntervalSerializerDeserializer.getIntervalEnd(bytes0, offset0 + 1);
                            intervalTypeTag = AIntervalSerializerDeserializer.getIntervalTimeType(bytes0, offset0 + 1);
                            if (intervalTypeTag == ATypeTag.SERIALIZED_DATE_TYPE_TAG) {
                                intervalStart = intervalStart * GregorianCalendarSystem.CHRONON_OF_DAY;
                            }
                        } else {
                            throw new AlgebricksException(getIdentifier().getName()
                                    + ": the first argument should be INTERVAL/NULL/MISSING but got " + type0);
                        }

                        // get the anchor instance time
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();
                        ATypeTag type1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);
                        if (intervalTypeTag != bytes1[offset1]) {
                            throw new AlgebricksException(getIdentifier().getName() + ": expecting compatible type to "
                                    + type0 + "(" + intervalTypeTag + ") for the second argument but got " + type1);
                        }

                        long anchorTime;
                        switch (type1) {
                            case DATE:
                                anchorTime = ADateSerializerDeserializer.getChronon(bytes1, offset1 + 1)
                                        * GregorianCalendarSystem.CHRONON_OF_DAY;
                                break;
                            case TIME:
                                anchorTime = ATimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                break;
                            case DATETIME:
                                anchorTime = ADateTimeSerializerDeserializer.getChronon(bytes1, offset1 + 1);
                                break;
                            default:
                                throw new AlgebricksException(
                                        getIdentifier().getName() + ": expecting compatible type to " + type0 + "("
                                                + intervalTypeTag + ") for the second argument but got " + type1);
                        }

                        byte[] bytes2 = argPtr2.getByteArray();
                        int offset2 = argPtr2.getStartOffset();

                        ATypeTag type2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes2[offset2]);

                        int yearMonth = 0;
                        long dayTime = 0;
                        long firstBinIndex;
                        switch (type2) {
                            case YEARMONTHDURATION:
                                yearMonth = AYearMonthDurationSerializerDeserializer.getYearMonth(bytes2, offset2 + 1);

                                int yearStart = gregCalSys.getYear(anchorTime);
                                int monthStart = gregCalSys.getMonthOfYear(anchorTime, yearStart);
                                int yearToBin = gregCalSys.getYear(intervalStart);
                                int monthToBin = gregCalSys.getMonthOfYear(intervalStart, yearToBin);

                                int totalMonths = (yearToBin - yearStart) * 12 + (monthToBin - monthStart);

                                firstBinIndex = totalMonths / yearMonth
                                        + ((totalMonths < 0 && totalMonths % yearMonth != 0) ? -1 : 0);

                                if (firstBinIndex > Integer.MAX_VALUE) {
                                    throw new AlgebricksException(
                                            getIdentifier().getName() + ": Overflowing time value to be binned!");
                                }

                                if (firstBinIndex < Integer.MIN_VALUE) {
                                    throw new AlgebricksException(
                                            getIdentifier().getName() + ": Underflowing time value to be binned!");
                                }
                                break;

                            case DAYTIMEDURATION:
                                dayTime = ADayTimeDurationSerializerDeserializer.getDayTime(bytes2, offset2 + 1);

                                long totalChronon = intervalStart - anchorTime;

                                firstBinIndex = totalChronon / dayTime
                                        + ((totalChronon < 0 && totalChronon % dayTime != 0) ? -1 : 0);
                                break;
                            default:
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": expecting YEARMONTHDURATION/DAYTIMEDURATION for the thrid argument but got "
                                        + type2);
                        }

                        long binStartChronon;
                        long binEndChronon;
                        int binOffset;

                        listBuilder.reset(intListType);

                        try {
                            if (intervalTypeTag == ATypeTag.SERIALIZED_DATE_TYPE_TAG) {

                                binOffset = 0;

                                do {
                                    binStartChronon = DurationArithmeticOperations.addDuration(anchorTime,
                                            yearMonth * (int) (firstBinIndex + binOffset),
                                            dayTime * (firstBinIndex + binOffset), false);
                                    binEndChronon = DurationArithmeticOperations.addDuration(anchorTime,
                                            yearMonth * ((int) (firstBinIndex + binOffset) + 1),
                                            dayTime * ((firstBinIndex + binOffset) + 1), false);
                                    binStartChronon = binStartChronon / GregorianCalendarSystem.CHRONON_OF_DAY
                                            + ((binStartChronon < 0
                                                    && binStartChronon % GregorianCalendarSystem.CHRONON_OF_DAY != 0)
                                                            ? -1 : 0);
                                    binEndChronon = binEndChronon / GregorianCalendarSystem.CHRONON_OF_DAY
                                            + ((binEndChronon < 0
                                                    && binEndChronon % GregorianCalendarSystem.CHRONON_OF_DAY != 0) ? -1
                                                            : 0);
                                    aInterval.setValue(binStartChronon, binEndChronon, intervalTypeTag);
                                    listStorage.reset();
                                    intervalSerde.serialize(aInterval, listStorage.getDataOutput());
                                    listBuilder.addItem(listStorage);
                                    binOffset++;
                                } while (binEndChronon < intervalEnd);

                            } else if (intervalTypeTag == ATypeTag.SERIALIZED_TIME_TYPE_TAG) {
                                if (yearMonth != 0) {
                                    throw new AlgebricksException(getIdentifier().getName()
                                            + ": cannot create year-month bin for a time value");
                                }

                                binOffset = 0;

                                binStartChronon = DurationArithmeticOperations.addDuration(anchorTime,
                                        yearMonth * (int) (firstBinIndex + binOffset),
                                        dayTime * (firstBinIndex + binOffset), true);
                                binEndChronon = DurationArithmeticOperations.addDuration(anchorTime,
                                        yearMonth * ((int) (firstBinIndex + binOffset) + 1),
                                        dayTime * ((firstBinIndex + binOffset) + 1), true);

                                if (binStartChronon < 0 || binStartChronon >= GregorianCalendarSystem.CHRONON_OF_DAY) {
                                    // avoid the case where a time bin is before 00:00:00 or no early than 24:00:00
                                    throw new AlgebricksException(getIdentifier().getName()
                                            + ": reaches a bin with the end earlier than the start; probably the window is beyond the time scope. Maybe use DATETIME?");
                                }

                                while (!((binStartChronon < intervalStart && binEndChronon <= intervalStart)
                                        || (binStartChronon >= intervalEnd && binEndChronon > intervalEnd))) {

                                    aInterval.setValue(binStartChronon, binEndChronon, intervalTypeTag);
                                    listStorage.reset();
                                    intervalSerde.serialize(aInterval, listStorage.getDataOutput());
                                    listBuilder.addItem(listStorage);
                                    binOffset++;
                                    binStartChronon = DurationArithmeticOperations.addDuration(anchorTime,
                                            yearMonth * (int) (firstBinIndex + binOffset),
                                            dayTime * (firstBinIndex + binOffset), true);
                                    binEndChronon = DurationArithmeticOperations.addDuration(anchorTime,
                                            yearMonth * ((int) (firstBinIndex + binOffset) + 1),
                                            dayTime * ((firstBinIndex + binOffset) + 1), true);

                                    if (binStartChronon == GregorianCalendarSystem.CHRONON_OF_DAY) {
                                        break;
                                    }

                                    if (binEndChronon < binStartChronon) {
                                        throw new AlgebricksException(getIdentifier().getName()
                                                + ": reaches a bin with the end earlier than the start; probably the window is beyond the time scope. Maybe use DATETIME?");
                                    }
                                }
                            } else if (intervalTypeTag == ATypeTag.SERIALIZED_DATETIME_TYPE_TAG) {
                                binOffset = 0;
                                do {
                                    binStartChronon = DurationArithmeticOperations.addDuration(anchorTime,
                                            yearMonth * (int) (firstBinIndex + binOffset),
                                            dayTime * (firstBinIndex + binOffset), false);
                                    binEndChronon = DurationArithmeticOperations.addDuration(anchorTime,
                                            yearMonth * ((int) (firstBinIndex + binOffset) + 1),
                                            dayTime * ((firstBinIndex + binOffset) + 1), false);
                                    aInterval.setValue(binStartChronon, binEndChronon, intervalTypeTag);
                                    listStorage.reset();
                                    intervalSerde.serialize(aInterval, listStorage.getDataOutput());
                                    listBuilder.addItem(listStorage);
                                    binOffset++;
                                } while (binEndChronon < intervalEnd);
                            } else {
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": the first argument should be DATE/TIME/DATETIME/NULL but got " + type0);
                            }
                            listBuilder.write(out, true);
                        } catch (IOException e1) {
                            throw new AlgebricksException(e1.getMessage());
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.OVERLAP_BINS;
    }

}
