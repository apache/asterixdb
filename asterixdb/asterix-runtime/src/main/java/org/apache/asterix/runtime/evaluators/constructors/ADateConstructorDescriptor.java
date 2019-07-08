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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
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
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable inputArg = new VoidPointable();
                    private IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);
                    private AMutableDate aDate = new AMutableDate(0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ADate> dateSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);

                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        try {
                            resultStorage.reset();
                            eval.evaluate(tuple, inputArg);

                            if (PointableHelper.checkAndSetMissingOrNull(result, inputArg)) {
                                return;
                            }

                            byte[] serString = inputArg.getByteArray();
                            int offset = inputArg.getStartOffset();
                            int len = inputArg.getLength();

                            byte tt = serString[offset];
                            if (tt == ATypeTag.SERIALIZED_DATE_TYPE_TAG) {
                                result.set(inputArg);
                            } else if (tt == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                utf8Ptr.set(serString, offset + 1, len - 1);
                                int stringLength = utf8Ptr.getUTF8Length();

                                // the string to be parsed should be at least 8 characters: YYYYMMDD
                                if (stringLength < 8) {
                                    throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                            ATypeTag.SERIALIZED_DATE_TYPE_TAG);
                                }

                                int startOffset = utf8Ptr.getCharStartOffset();
                                while (serString[startOffset] == ' ') {
                                    startOffset++;
                                }
                                int endOffset = startOffset + stringLength - 1;
                                while (serString[endOffset] == ' ') {
                                    endOffset--;
                                }

                                long chrononTimeInMs = ADateParserFactory.parseDatePart(serString, startOffset,
                                        endOffset - startOffset + 1);
                                short temp = 0;
                                if (chrononTimeInMs < 0
                                        && chrononTimeInMs % GregorianCalendarSystem.CHRONON_OF_DAY != 0) {
                                    temp = 1;
                                }
                                aDate.setValue((int) (chrononTimeInMs / GregorianCalendarSystem.CHRONON_OF_DAY) - temp);
                                dateSerde.serialize(aDate, out);
                                result.set(resultStorage);
                            } else {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, tt,
                                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            }
                        } catch (IOException e) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(), e,
                                    ATypeTag.SERIALIZED_DATE_TYPE_TAG);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.DATE_CONSTRUCTOR;
    }

}
