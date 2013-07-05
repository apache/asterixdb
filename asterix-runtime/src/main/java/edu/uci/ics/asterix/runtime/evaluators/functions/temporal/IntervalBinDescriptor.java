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
package edu.uci.ics.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInterval;
import edu.uci.ics.asterix.om.base.AMutableInterval;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.temporal.DurationArithmeticOperations;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class IntervalBinDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new IntervalBinDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private final DataOutput out = output.getDataOutput();

                    private final ArrayBackedValueStorage argOut0 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage argOut1 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage argOut2 = new ArrayBackedValueStorage();

                    private final ICopyEvaluator eval0 = args[0].createEvaluator(argOut0);
                    private final ICopyEvaluator eval1 = args[1].createEvaluator(argOut1);
                    private final ICopyEvaluator eval2 = args[2].createEvaluator(argOut2);

                    private final AMutableInterval aInterval = new AMutableInterval(0, 0, (byte) -1);

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInterval> intervalSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINTERVAL);

                    private final GregorianCalendarSystem GREG_CAL = GregorianCalendarSystem.getInstance();

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut0.reset();
                        eval0.evaluate(tuple);

                        ATypeTag type0 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut0.getByteArray()[0]);

                        long chrononToBin = 0;
                        switch (type0) {
                            case DATE:
                                chrononToBin = ADateSerializerDeserializer.getChronon(argOut0.getByteArray(), 1)
                                        * GregorianCalendarSystem.CHRONON_OF_DAY;
                                break;
                            case TIME:
                                chrononToBin = ATimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                                break;
                            case DATETIME:
                                chrononToBin = ADateTimeSerializerDeserializer.getChronon(argOut0.getByteArray(), 1);
                                break;
                            case NULL:
                                try {
                                    nullSerde.serialize(ANull.NULL, out);
                                } catch (HyracksDataException e) {
                                    throw new AlgebricksException(e);
                                }
                                return;
                            default:
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": the first argument should be DATE/TIME/DATETIME/NULL but got " + type0);

                        }

                        argOut1.reset();
                        eval1.evaluate(tuple);

                        ATypeTag type1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut1.getByteArray()[0]);

                        if (type0 != type1) {
                            if (type0 != ATypeTag.NULL && type1 != ATypeTag.NULL)
                                throw new AlgebricksException(getIdentifier().getName() + ": expecting " + type0
                                        + " for the second argument but got " + type1);
                        }

                        long chrononToStart = 0;
                        switch (type1) {
                            case DATE:
                                chrononToStart = ADateSerializerDeserializer.getChronon(argOut1.getByteArray(), 1)
                                        * GregorianCalendarSystem.CHRONON_OF_DAY;
                                break;
                            case TIME:
                                chrononToStart = ATimeSerializerDeserializer.getChronon(argOut1.getByteArray(), 1);
                                break;
                            case DATETIME:
                                chrononToStart = ADateTimeSerializerDeserializer.getChronon(argOut1.getByteArray(), 1);
                                break;
                            case NULL:
                                try {
                                    nullSerde.serialize(ANull.NULL, out);
                                } catch (HyracksDataException e) {
                                    throw new AlgebricksException(e);
                                }
                                return;
                            default:
                                throw new AlgebricksException(getIdentifier().getName() + ": expecting " + type0
                                        + " for the second argument but got " + type1);
                        }

                        argOut2.reset();
                        eval2.evaluate(tuple);

                        ATypeTag type2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut2.getByteArray()[0]);

                        int yearMonth = 0;
                        long dayTime = 0;
                        long binIndex, binStartChronon, binEndChronon;
                        switch (type2) {
                            case YEARMONTHDURATION:

                                yearMonth = AYearMonthDurationSerializerDeserializer.getYearMonth(
                                        argOut2.getByteArray(), 1);

                                int yearStart = GREG_CAL.getYear(chrononToStart);
                                int monthStart = GREG_CAL.getMonthOfYear(chrononToStart, yearStart);
                                int yearToBin = GREG_CAL.getYear(chrononToBin);
                                int monthToBin = GREG_CAL.getMonthOfYear(chrononToBin, yearToBin);

                                int totalMonths = (yearToBin - yearStart) * 12 + (monthToBin - monthStart);

                                binIndex = totalMonths / yearMonth
                                        + ((totalMonths < 0 && totalMonths % yearMonth != 0) ? -1 : 0);

                                if (binIndex > Integer.MAX_VALUE) {
                                    throw new AlgebricksException(getIdentifier().getName()
                                            + ": Overflowing time value to be binned!");
                                }

                                if (binIndex < Integer.MIN_VALUE) {
                                    throw new AlgebricksException(getIdentifier().getName()
                                            + ": Underflowing time value to be binned!");
                                }

                                break;

                            case DAYTIMEDURATION:
                                dayTime = ADayTimeDurationSerializerDeserializer.getDayTime(argOut2.getByteArray(), 1);

                                long totalChronon = chrononToBin - chrononToStart;

                                binIndex = totalChronon / dayTime
                                        + ((totalChronon < 0 && totalChronon % dayTime != 0) ? -1 : 0);

                                break;
                            case NULL:
                                try {
                                    nullSerde.serialize(ANull.NULL, out);
                                } catch (HyracksDataException e) {
                                    throw new AlgebricksException(e);
                                }
                                return;
                            default:
                                throw new AlgebricksException(
                                        getIdentifier().getName()
                                                + ": expecting YEARMONTHDURATION/DAYTIMEDURATION for the thrid argument but got "
                                                + type2);
                        }

                        switch (type0) {
                            case DATE:
                                binStartChronon = DurationArithmeticOperations.addDuration(chrononToStart, yearMonth
                                        * (int) binIndex, dayTime * binIndex, false);
                                binEndChronon = DurationArithmeticOperations.addDuration(chrononToStart, yearMonth
                                        * ((int) binIndex + 1), dayTime * (binIndex + 1), false);

                                binStartChronon = binStartChronon
                                        / GregorianCalendarSystem.CHRONON_OF_DAY
                                        + ((binStartChronon < 0 && binStartChronon
                                                % GregorianCalendarSystem.CHRONON_OF_DAY != 0) ? -1 : 0);
                                binEndChronon = binEndChronon
                                        / GregorianCalendarSystem.CHRONON_OF_DAY
                                        + ((binEndChronon < 0 && binEndChronon % GregorianCalendarSystem.CHRONON_OF_DAY != 0) ? -1
                                                : 0);
                                break;
                            case TIME:
                                if (yearMonth != 0) {
                                    throw new AlgebricksException(getIdentifier().getName()
                                            + ": cannot create year-month bin for a time value");
                                }
                                binStartChronon = DurationArithmeticOperations.addDuration(chrononToStart, yearMonth
                                        * (int) binIndex, dayTime * binIndex, true);
                                binEndChronon = DurationArithmeticOperations.addDuration(chrononToStart, yearMonth
                                        * ((int) binIndex + 1), dayTime * (binIndex + 1), true);
                                break;
                            case DATETIME:
                                binStartChronon = DurationArithmeticOperations.addDuration(chrononToStart, yearMonth
                                        * (int) binIndex, dayTime * binIndex, false);
                                binEndChronon = DurationArithmeticOperations.addDuration(chrononToStart, yearMonth
                                        * ((int) binIndex + 1), dayTime * (binIndex + 1), false);
                                break;
                            case NULL:
                                try {
                                    nullSerde.serialize(ANull.NULL, out);
                                } catch (HyracksDataException e) {
                                    throw new AlgebricksException(e);
                                }
                                return;
                            default:
                                throw new AlgebricksException(getIdentifier().getName()
                                        + ": the first argument should be DATE/TIME/DATETIME/NULL but got " + type0);

                        }
                        aInterval.setValue(binStartChronon, binEndChronon, type0.serialize());
                        try {
                            intervalSerde.serialize(aInterval, out);
                            return;
                        } catch (HyracksDataException ex) {
                            throw new AlgebricksException(ex);
                        }

                    }
                };
            }

        };
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.functions.AbstractFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.INTERVAL_BIN;
    }

}
