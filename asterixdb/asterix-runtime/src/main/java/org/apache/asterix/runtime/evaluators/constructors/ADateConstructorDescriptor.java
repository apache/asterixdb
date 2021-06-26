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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.temporal.ADateParserFactory;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

@MissingNullInOutFunction
public class ADateConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ADateConstructorDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new AbstractConstructorEvaluator(ctx, args[0].createScalarEvaluator(ctx), sourceLoc) {

                    private final AMutableDate aDate = new AMutableDate(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ADate> dateSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);
                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

                    @Override
                    protected void evaluateImpl(IPointable result) throws HyracksDataException {
                        byte[] bytes = inputArg.getByteArray();
                        int startOffset = inputArg.getStartOffset();
                        int len = inputArg.getLength();
                        ATypeTag inputType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[startOffset]);
                        switch (inputType) {
                            case DATE:
                                result.set(inputArg);
                                break;
                            case DATETIME:
                                long chronon = ADateTimeSerializerDeserializer.getChronon(bytes, startOffset + 1);
                                aDate.setValue(GregorianCalendarSystem.getInstance().getChrononInDays(chronon));
                                resultStorage.reset();
                                dateSerde.serialize(aDate, out);
                                result.set(resultStorage);
                                break;
                            case STRING:
                                utf8Ptr.set(bytes, startOffset + 1, len - 1);
                                if (parseDate(utf8Ptr, aDate)) {
                                    resultStorage.reset();
                                    dateSerde.serialize(aDate, out);
                                    result.set(resultStorage);
                                } else {
                                    handleParseError(utf8Ptr, result);
                                }
                                break;
                            default:
                                handleUnsupportedType(inputType, result);
                                break;
                        }
                    }

                    @Override
                    protected FunctionIdentifier getIdentifier() {
                        return ADateConstructorDescriptor.this.getIdentifier();
                    }

                    @Override
                    protected BuiltinType getTargetType() {
                        return BuiltinType.ADATE;
                    }
                };
            }
        };
    }

    private static boolean parseDate(UTF8StringPointable textPtr, AMutableDate result) {
        int stringLength = textPtr.getUTF8Length();
        if (stringLength < 8) {
            return false;
        }
        try {
            long chronon = ADateParserFactory.parseDatePart(textPtr.getByteArray(), textPtr.getCharStartOffset(),
                    stringLength);
            int chrononInDays = GregorianCalendarSystem.getInstance().getChrononInDays(chronon);
            result.setValue(chrononInDays);
            return true;
        } catch (HyracksDataException e) {
            return false;
        }
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.DATE_CONSTRUCTOR;
    }
}
