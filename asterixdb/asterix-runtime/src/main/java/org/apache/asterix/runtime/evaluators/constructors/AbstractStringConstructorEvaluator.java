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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractStringConstructorEvaluator implements IScalarEvaluator {

    protected final IScalarEvaluator inputEval;
    protected final SourceLocation sourceLoc;
    protected final IPointable inputArg;
    protected final ArrayBackedValueStorage resultStorage;
    protected final DataOutput out;
    protected final UTF8StringBuilder builder;
    protected final GrowableArray baaos;

    protected AbstractStringConstructorEvaluator(IScalarEvaluator inputEval, SourceLocation sourceLoc) {
        this.inputEval = inputEval;
        this.sourceLoc = sourceLoc;
        resultStorage = new ArrayBackedValueStorage();
        out = resultStorage.getDataOutput();
        inputArg = new VoidPointable();
        builder = new UTF8StringBuilder();
        baaos = new GrowableArray();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        try {
            inputEval.evaluate(tuple, inputArg);
            resultStorage.reset();

            if (PointableHelper.checkAndSetMissingOrNull(result, inputArg)) {
                return;
            }

            evaluateImpl(result);
        } catch (IOException e) {
            throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e, ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
    }

    protected void evaluateImpl(IPointable result) throws IOException {
        byte[] serString = inputArg.getByteArray();
        int offset = inputArg.getStartOffset();

        ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[serString[offset]];
        if (tt == ATypeTag.STRING) {
            result.set(inputArg);
        } else {
            int len = inputArg.getLength();
            baaos.reset();
            builder.reset(baaos, len);
            int startOffset = offset + 1;
            switch (tt) {
                case TINYINT: {
                    int i = AInt8SerializerDeserializer.getByte(serString, startOffset);
                    builder.appendString(String.valueOf(i));
                    break;
                }
                case SMALLINT: {
                    int i = AInt16SerializerDeserializer.getShort(serString, startOffset);
                    builder.appendString(String.valueOf(i));
                    break;
                }
                case INTEGER: {
                    int i = AInt32SerializerDeserializer.getInt(serString, startOffset);
                    builder.appendString(String.valueOf(i));
                    break;
                }
                case BIGINT: {
                    long l = AInt64SerializerDeserializer.getLong(serString, startOffset);
                    builder.appendString(String.valueOf(l));
                    break;
                }
                case DOUBLE: {
                    double d = ADoubleSerializerDeserializer.getDouble(serString, startOffset);
                    if (Double.isNaN(d)) {
                        builder.appendUtf8StringPointable(NumberUtils.NAN);
                    } else if (d == Double.POSITIVE_INFINITY) { // NOSONAR
                        builder.appendUtf8StringPointable(NumberUtils.POSITIVE_INF);
                    } else if (d == Double.NEGATIVE_INFINITY) { // NOSONAR
                        builder.appendUtf8StringPointable(NumberUtils.NEGATIVE_INF);
                    } else {
                        builder.appendString(String.valueOf(d));
                    }
                    break;
                }
                case FLOAT: {
                    float f = AFloatSerializerDeserializer.getFloat(serString, startOffset);
                    if (Float.isNaN(f)) {
                        builder.appendUtf8StringPointable(NumberUtils.NAN);
                    } else if (f == Float.POSITIVE_INFINITY) { // NOSONAR
                        builder.appendUtf8StringPointable(NumberUtils.POSITIVE_INF);
                    } else if (f == Float.NEGATIVE_INFINITY) { // NOSONAR
                        builder.appendUtf8StringPointable(NumberUtils.NEGATIVE_INF);
                    } else {
                        builder.appendString(String.valueOf(f));
                    }
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
                case ARRAY:
                case POINT:
                case POINT3D:
                case RECTANGLE:
                case POLYGON:
                case OBJECT:
                case MULTISET:
                case UUID:
                default:
                    throw new UnsupportedTypeException(sourceLoc, getIdentifier(), serString[offset]);
            }
            builder.finish();
            out.write(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            out.write(baaos.getByteArray(), 0, baaos.getLength());
            result.set(resultStorage);
        }
    }

    protected abstract FunctionIdentifier getIdentifier();
}
