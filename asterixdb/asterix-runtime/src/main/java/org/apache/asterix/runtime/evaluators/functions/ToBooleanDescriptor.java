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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.NumberUtils;
import org.apache.asterix.runtime.evaluators.constructors.AbstractBooleanConstructorEvaluator;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

@MissingNullInOutFunction
public class ToBooleanDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ToBooleanDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractBooleanConstructorEvaluator(args[0].createScalarEvaluator(ctx), sourceLoc) {
                    @Override
                    protected void evaluateImpl(IPointable result) throws HyracksDataException {
                        byte[] bytes = inputArg.getByteArray();
                        int offset = inputArg.getStartOffset();

                        ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[bytes[offset]];
                        switch (tt) {
                            case TINYINT:
                                setInteger(AInt8SerializerDeserializer.getByte(bytes, offset + 1), result);
                                break;
                            case SMALLINT:
                                setInteger(AInt16SerializerDeserializer.getShort(bytes, offset + 1), result);
                                break;
                            case INTEGER:
                                setInteger(AInt32SerializerDeserializer.getInt(bytes, offset + 1), result);
                                break;
                            case BIGINT:
                                setInteger(AInt64SerializerDeserializer.getLong(bytes, offset + 1), result);
                                break;
                            case FLOAT:
                                setDouble(AFloatSerializerDeserializer.getFloat(bytes, offset + 1), result);
                                break;
                            case DOUBLE:
                                setDouble(ADoubleSerializerDeserializer.getDouble(bytes, offset + 1), result);
                                break;
                            case STRING:
                                setInteger(UTF8StringUtil.getStringLength(bytes, offset + 1), result);
                                break;
                            case ARRAY:
                                setInteger(AOrderedListSerializerDeserializer.getNumberOfItems(bytes, offset), result);
                                break;
                            case MULTISET:
                                setInteger(AUnorderedListSerializerDeserializer.getNumberOfItems(bytes, offset),
                                        result);
                                break;
                            case OBJECT:
                                setBoolean(result, !ARecordSerializerDeserializer.hasNoFields(bytes, offset,
                                        inputArg.getLength()));
                                break;
                            default:
                                super.evaluateImpl(result);
                                break;
                        }
                    }

                    private void setInteger(long v, IPointable result) throws HyracksDataException {
                        setBoolean(result, v != 0);
                    }

                    private void setDouble(double v, IPointable result) throws HyracksDataException {
                        long bits = Double.doubleToLongBits(v);
                        boolean zeroOrNaN = bits == NumberUtils.POSITIVE_ZERO_BITS
                                || bits == NumberUtils.NEGATIVE_ZERO_BITS || bits == NumberUtils.NAN_BITS;
                        setBoolean(result, !zeroOrNaN);
                    }

                    @Override
                    protected FunctionIdentifier getIdentifier() {
                        return ToBooleanDescriptor.this.getIdentifier();
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.TO_BOOLEAN;
    }
}
