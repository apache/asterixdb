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
package edu.uci.ics.asterix.runtime.evaluators.accessors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.ATime;
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
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TemporalIntervalEndAccessor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final FunctionIdentifier FID = AsterixBuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END;

    private static final byte SER_INTERVAL_TYPE_TAG = ATypeTag.INTERVAL.serialize();
    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    private static final byte SER_DATE_TYPE_TAG = ATypeTag.DATE.serialize();
    private static final byte SER_TIME_TYPE_TAG = ATypeTag.TIME.serialize();
    private static final byte SER_DATETIME_TYPE_TAG = ATypeTag.DATETIME.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new TemporalIntervalEndAccessor();
        }
    };

    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();

                    private ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

                    private ICopyEvaluator eval = args[0].createEvaluator(argOut);

                    // possible output
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADate> dateSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADATE);
                    private AMutableDate aDate = new AMutableDate(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADateTime> datetimeSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADATETIME);
                    private AMutableDateTime aDateTime = new AMutableDateTime(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ATime> timeSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ATIME);
                    private AMutableTime aTime = new AMutableTime(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        eval.evaluate(tuple);
                        byte[] bytes = argOut.getByteArray();

                        try {
                            if (bytes[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            } else if (bytes[0] == SER_INTERVAL_TYPE_TAG) {
                                byte timeType = AIntervalSerializerDeserializer.getIntervalTimeType(bytes, 1);
                                long endTime = AIntervalSerializerDeserializer.getIntervalEnd(bytes, 1);
                                if (timeType == SER_DATE_TYPE_TAG) {
                                    aDate.setValue((int) (endTime));
                                    dateSerde.serialize(aDate, out);
                                } else if (timeType == SER_TIME_TYPE_TAG) {
                                    aTime.setValue((int) (endTime));
                                    timeSerde.serialize(aTime, out);
                                } else if (timeType == SER_DATETIME_TYPE_TAG) {
                                    aDateTime.setValue(endTime);
                                    datetimeSerde.serialize(aDateTime, out);
                                }
                            } else {
                                throw new AlgebricksException(FID.getName() + ": expects NULL/INTERVAL, but got "
                                        + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[0]));
                            }
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
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
        return FID;
    }

}
