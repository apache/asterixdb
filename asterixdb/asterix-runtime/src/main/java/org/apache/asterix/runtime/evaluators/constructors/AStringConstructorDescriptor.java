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

import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AStringConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AStringConstructorDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable inputArg = new VoidPointable();
                    private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
                    private UTF8StringBuilder builder = new UTF8StringBuilder();
                    private GrowableArray baaos = new GrowableArray();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            resultStorage.reset();
                            baaos.reset();
                            eval.evaluate(tuple, inputArg);
                            byte[] serString = inputArg.getByteArray();
                            int offset = inputArg.getStartOffset();
                            int len = inputArg.getLength();

                            ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[serString[offset]];
                            if (tt == ATypeTag.STRING) {
                                result.set(inputArg);
                            } else {
                                builder.reset(baaos, len);
                                int startOffset = offset + 1;
                                switch (tt) {
                                    case INT8: {
                                        int i = AInt8SerializerDeserializer.getByte(serString, startOffset);
                                        builder.appendString(String.valueOf(i));
                                        break;
                                    }
                                    case INT16: {
                                        int i = AInt16SerializerDeserializer.getShort(serString, startOffset);
                                        builder.appendString(String.valueOf(i));
                                        break;
                                    }
                                    case INT32: {
                                        int i = AInt32SerializerDeserializer.getInt(serString, startOffset);
                                        builder.appendString(String.valueOf(i));
                                        break;
                                    }
                                    case INT64: {
                                        long l = AInt64SerializerDeserializer.getLong(serString, startOffset);
                                        builder.appendString(String.valueOf(l));
                                        break;
                                    }
                                    case DOUBLE: {
                                        double d = ADoubleSerializerDeserializer.getDouble(serString, startOffset);
                                        builder.appendString(String.valueOf(d));
                                        break;
                                    }
                                    case FLOAT: {
                                        float f = AFloatSerializerDeserializer.getFloat(serString, startOffset);
                                        builder.appendString(String.valueOf(f));
                                        break;
                                    }
                                    case BOOLEAN: {
                                        boolean b = ABooleanSerializerDeserializer.getBoolean(serString, startOffset);
                                        builder.appendString(String.valueOf(b));
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
                                        throw new UnsupportedTypeException(getIdentifier(), serString[offset]);
                                }
                                builder.finish();
                                out.write(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                                out.write(baaos.getByteArray(), 0, baaos.getLength());
                                result.set(resultStorage);
                            }
                        } catch (IOException e) {
                            throw new InvalidDataFormatException(getIdentifier(), e,
                                    ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.STRING_CONSTRUCTOR;
    }

}
