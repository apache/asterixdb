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
package org.apache.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableUUID;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Receives a canonical representation of UUID and construct a UUID value.
 * a UUID is represented by 32 lowercase hexadecimal digits (8-4-4-4-12). (E.g.
 * uuid("02a199ca-bf58-412e-bd9f-60a0c975a8ac"))
 */
public class AUUIDFromStringConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
    private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AUUIDFromStringConstructorDescriptor();
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
                    private String errorMessage = "This can not be an instance of UUID";
                    private AMutableUUID aUUID = new AMutableUUID(0, 0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AUUID> uuidSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AUUID);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    private long msb = 0;
                    private long lsb = 0;
                    private long tmpLongValue = 0;

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            outInput.reset();
                            eval.evaluate(tuple);
                            byte[] serString = outInput.getByteArray();
                            if (serString[0] == SER_STRING_TYPE_TAG) {
                                msb = 0;
                                lsb = 0;
                                tmpLongValue = 0;

                                // first byte: tag, next two bytes: length, so
                                // we add 3 bytes.
                                // First part - 8 bytes
                                int offset = 3;
                                msb = calculateLongFromHex(serString, offset, 8);
                                msb <<= 16;
                                offset += 8;

                                // Skip the hyphen part
                                offset += 1;

                                // Second part - 4 bytes
                                tmpLongValue = calculateLongFromHex(serString, offset, 4);
                                msb |= tmpLongValue;
                                msb <<= 16;
                                offset += 4;

                                // Skip the hyphen part
                                offset += 1;

                                // Third part - 4 bytes
                                tmpLongValue = calculateLongFromHex(serString, offset, 4);
                                msb |= tmpLongValue;
                                offset += 4;

                                // Skip the hyphen part
                                offset += 1;

                                // Fourth part - 4 bytes
                                lsb = calculateLongFromHex(serString, offset, 4);
                                lsb <<= 48;
                                offset += 4;

                                // Skip the hyphen part
                                offset += 1;

                                // The last part - 12 bytes
                                tmpLongValue = calculateLongFromHex(serString, offset, 12);
                                lsb |= tmpLongValue;

                                aUUID.setValue(msb, lsb);
                                uuidSerde.serialize(aUUID, out);

                            } else if (serString[0] == SER_NULL_TYPE_TAG)
                                nullSerde.serialize(ANull.NULL, out);
                            else
                                throw new AlgebricksException(errorMessage);
                        } catch (IOException e1) {
                            throw new AlgebricksException(errorMessage);
                        }
                    }

                    // Calculate a long value from a hex string.
                    private long calculateLongFromHex(byte[] hexArray, int offset, int length)
                            throws AlgebricksException {
                        int tmpIntVal = 0;
                        long tmpLongVal = 0;
                        for (int i = offset; i < offset + length; i++) {
                            tmpIntVal = transformHexCharToInt(hexArray[i]);
                            if (tmpIntVal != -1) {
                                tmpLongVal = tmpLongVal * 16 + tmpIntVal;
                            } else {
                                throw new AlgebricksException("This is not a correct UUID value.");
                            }
                        }
                        return tmpLongVal;
                    }

                    // Interpret a character to the corresponding integer value.
                    private int transformHexCharToInt(byte val) throws AlgebricksException {
                        switch (val) {
                            case '0':
                                return 0;
                            case '1':
                                return 1;
                            case '2':
                                return 2;
                            case '3':
                                return 3;
                            case '4':
                                return 4;
                            case '5':
                                return 5;
                            case '6':
                                return 6;
                            case '7':
                                return 7;
                            case '8':
                                return 8;
                            case '9':
                                return 9;
                            case 'a':
                            case 'A':
                                return 10;
                            case 'b':
                            case 'B':
                                return 11;
                            case 'c':
                            case 'C':
                                return 12;
                            case 'd':
                            case 'D':
                                return 13;
                            case 'e':
                            case 'E':
                                return 14;
                            case 'f':
                            case 'F':
                                return 15;
                            case '-':
                                // We need to skip this hyphen part.
                                return -1;
                            default:
                                throw new AlgebricksException("This is not a correct UUID value.");
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.UUID_CONSTRUCTOR;
    }

}