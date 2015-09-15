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
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
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
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AStringConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AStringConstructorDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                try {
                    return new ICopyEvaluator() {

                        private DataOutput out = output.getDataOutput();
                        private ArrayBackedValueStorage outInput = new ArrayBackedValueStorage();
                        private ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
                        private PrintStream ps = new PrintStream(baaos, false, "UTF-8");
                        private ICopyEvaluator eval = args[0].createEvaluator(outInput);
                        @SuppressWarnings("unchecked")
                        private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                                .getSerializerDeserializer(BuiltinType.ANULL);

                        @Override
                        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                            try {
                                outInput.reset();
                                baaos.reset();
                                eval.evaluate(tuple);
                                byte[] serString = outInput.getByteArray();

                                ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[serString[0]];
                                if (tt == ATypeTag.NULL) {
                                    nullSerde.serialize(ANull.NULL, out);
                                } else if (tt == ATypeTag.STRING) {
                                    out.write(outInput.getByteArray(), outInput.getStartOffset(), outInput.getLength());
                                } else {
                                    baaos.write(0);
                                    baaos.write(0);
                                    switch (tt) {
                                        case INT8: {
                                            int i = AInt8SerializerDeserializer.getByte(outInput.getByteArray(), 1);
                                            ps.print(i);
                                            break;
                                        }
                                        case INT16: {
                                            int i = AInt16SerializerDeserializer.getShort(outInput.getByteArray(), 1);
                                            ps.print(i);
                                            break;
                                        }
                                        case INT32: {
                                            int i = AInt32SerializerDeserializer.getInt(outInput.getByteArray(), 1);
                                            ps.print(i);
                                            break;
                                        }
                                        case INT64: {
                                            long l = AInt64SerializerDeserializer.getLong(outInput.getByteArray(), 1);
                                            ps.print(l);
                                            break;
                                        }
                                        case DOUBLE: {
                                            double d = ADoubleSerializerDeserializer.getDouble(outInput.getByteArray(),
                                                    1);
                                            ps.print(d);
                                            break;
                                        }
                                        case FLOAT: {
                                            float f = AFloatSerializerDeserializer.getFloat(outInput.getByteArray(), 1);
                                            ps.print(f);
                                            break;
                                        }
                                        case BOOLEAN: {
                                            boolean b = ABooleanSerializerDeserializer.getBoolean(
                                                    outInput.getByteArray(), 1);
                                            ps.print(b);
                                            break;
                                        }

                                        // NotYetImplemented
                                        case CIRCLE:
                                        case DATE:
                                        case DATETIME:
                                        case LINE:
                                        case TIME:
                                        case DURATION:
                                        case YEARMONTHDURATION:
                                        case DAYTIMEDURATION:
                                        case INTERVAL:
                                        case ORDEREDLIST:
                                        case POINT:
                                        case POINT3D:
                                        case RECTANGLE:
                                        case POLYGON:
                                        case RECORD:
                                        case UNORDEREDLIST:
                                        case UUID:
                                        default:
                                            throw new AlgebricksException("string of " + tt + " not supported");
                                    }
                                    ps.flush();
                                    byte[] tmpStrBytes = baaos.getByteArray();
                                    int utfLen = baaos.size() - 2;
                                    tmpStrBytes[0] = (byte) ((utfLen >>> 8) & 0xFF);
                                    tmpStrBytes[1] = (byte) ((utfLen >>> 0) & 0xFF);
                                    out.write(ATypeTag.STRING.serialize());
                                    out.write(tmpStrBytes);
                                }
                            } catch (IOException e) {
                                throw new AlgebricksException(e);
                            }
                        }
                    };
                } catch (UnsupportedEncodingException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_CONSTRUCTOR;
    }

}