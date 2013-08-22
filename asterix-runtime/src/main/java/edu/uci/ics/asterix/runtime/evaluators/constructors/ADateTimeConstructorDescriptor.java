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
package edu.uci.ics.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.temporal.ADateParserFactory;
import edu.uci.ics.asterix.om.base.temporal.ATimeParserFactory;
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

public class ADateTimeConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ADateTimeConstructorDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();

                    private ArrayBackedValueStorage outInput = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval = args[0].createEvaluator(outInput);
                    private String errorMessage = "This can not be an instance of datetime";
                    private AMutableDateTime aDateTime = new AMutableDateTime(0L);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADateTime> datetimeSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ADATETIME);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

                        try {
                            outInput.reset();
                            eval.evaluate(tuple);
                            byte[] serString = outInput.getByteArray();
                            if (serString[0] == SER_STRING_TYPE_TAG) {

                                int stringLength = (serString[1] & 0xff << 8) + (serString[2] & 0xff << 0);
                                // the string to be parsed should be at least 14 characters: YYYYMMDDhhmmss
                                if (stringLength < 14) {
                                    throw new AlgebricksException(errorMessage
                                            + ": the string length should be at least 14 (YYYYMMDDhhmmss) but it is "
                                            + stringLength);
                                }
                                // +1 if it is negative (-)
                                short timeOffset = (short) ((serString[3] == '-') ? 1 : 0);

                                timeOffset += 8;

                                if (serString[3 + timeOffset] != 'T') {
                                    timeOffset += 2;
                                    if (serString[3 + timeOffset] != 'T') {
                                        throw new AlgebricksException(errorMessage + ": missing T");
                                    }
                                }

                                long chrononTimeInMs = ADateParserFactory.parseDatePart(serString, 3, timeOffset);

                                chrononTimeInMs += ATimeParserFactory.parseTimePart(serString, 3 + timeOffset + 1,
                                        stringLength - timeOffset - 1);

                                aDateTime.setValue(chrononTimeInMs);
                                datetimeSerde.serialize(aDateTime, out);
                            } else if (serString[0] == SER_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else {
                                throw new AlgebricksException(errorMessage);
                            }
                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.DATETIME_CONSTRUCTOR;
    }

}