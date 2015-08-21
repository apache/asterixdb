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

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class TemporalSecondAccessor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final FunctionIdentifier FID = AsterixBuiltinFunctions.ACCESSOR_TEMPORAL_SEC;

    // allowed input types
    private static final byte SER_TIME_TYPE_TAG = ATypeTag.TIME.serialize();
    private static final byte SER_DATETIME_TYPE_TAG = ATypeTag.DATETIME.serialize();
    private static final byte SER_DURATION_TYPE_TAG = ATypeTag.DURATION.serialize();
    private static final byte SER_DAY_TIME_DURATION_TYPE_TAG = ATypeTag.DAYTIMEDURATION.serialize();
    private static final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new TemporalSecondAccessor();
        }

    };

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.runtime.base.IScalarFunctionDynamicDescriptor#createEvaluatorFactory(edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory[])
     */
    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private final DataOutput out = output.getDataOutput();

                    private final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();

                    private final ICopyEvaluator eval = args[0].createEvaluator(argOut);

                    private final GregorianCalendarSystem calSystem = GregorianCalendarSystem.getInstance();

                    // for output: type integer
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt64> intSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT64);
                    private final AMutableInt64 aMutableInt64 = new AMutableInt64(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        argOut.reset();
                        eval.evaluate(tuple);
                        byte[] bytes = argOut.getByteArray();

                        try {

                            if (bytes[0] == SER_DURATION_TYPE_TAG) {
                                aMutableInt64.setValue(calSystem.getDurationSecond(ADurationSerializerDeserializer
                                        .getDayTime(bytes, 1)));
                                intSerde.serialize(aMutableInt64, out);
                                return;
                            }

                            if (bytes[0] == SER_DAY_TIME_DURATION_TYPE_TAG) {
                                aMutableInt64
                                        .setValue(calSystem.getDurationSecond(ADayTimeDurationSerializerDeserializer
                                                .getDayTime(bytes, 1)));
                                intSerde.serialize(aMutableInt64, out);
                                return;
                            }

                            long chrononTimeInMs = 0;
                            if (bytes[0] == SER_TIME_TYPE_TAG) {
                                chrononTimeInMs = AInt32SerializerDeserializer.getInt(bytes, 1);
                            } else if (bytes[0] == SER_DATETIME_TYPE_TAG) {
                                chrononTimeInMs = AInt64SerializerDeserializer.getLong(bytes, 1);
                            } else if (bytes[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                return;
                            } else {
                                throw new AlgebricksException("Inapplicable input type: " + bytes[0]);
                            }

                            int sec = calSystem.getSecOfMin(chrononTimeInMs);

                            aMutableInt64.setValue(sec);
                            intSerde.serialize(aMutableInt64, out);

                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.functions.IFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
