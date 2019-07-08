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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.temporal.DateTimeFormatUtils;
import org.apache.asterix.om.base.temporal.DateTimeFormatUtils.DateTimeParseMode;
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

/**
 * <b>|(bar)</b> is a special separator used to separate different formatting options.
 * Multiple format strings can be used by separating them using <b>|(bar)</b>, and the parsing will be successful only
 * when the format string has the <b>exact</b> match with the given data string.
 * This means that a time string like <it>08:23:12 AM</it> will not be valid for the format string <it>h:m:s</it>
 * as there is no AM/PM format character in the format string.
 */

@MissingNullInOutFunction
public class ParseDateDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ParseDateDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable argPtr0 = new VoidPointable();
                    private final IPointable argPtr1 = new VoidPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ADate> dateSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATE);

                    private final AMutableInt64 aInt64 = new AMutableInt64(0);
                    private final AMutableDate aDate = new AMutableDate(0);
                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

                    private final DateTimeFormatUtils util = DateTimeFormatUtils.getInstance();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
                            return;
                        }

                        byte[] bytes0 = argPtr0.getByteArray();
                        int offset0 = argPtr0.getStartOffset();
                        int len0 = argPtr0.getLength();
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();
                        int len1 = argPtr1.getLength();

                        if (bytes0[offset0] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                    ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                        }
                        if (bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                    ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                        }

                        utf8Ptr.set(bytes0, offset0 + 1, len0 - 1);
                        int start0 = utf8Ptr.getCharStartOffset();
                        int length0 = utf8Ptr.getUTF8Length();

                        utf8Ptr.set(bytes1, offset1 + 1, len1 - 1);
                        int start1 = utf8Ptr.getCharStartOffset();
                        int length1 = utf8Ptr.getUTF8Length();

                        int formatStart = start1;
                        int formatLength;
                        boolean processSuccessfully = false;
                        while (!processSuccessfully && formatStart < start1 + length1) {
                            // search for "|"
                            formatLength = 0;
                            for (; formatStart + formatLength < start1 + length1; formatLength++) {
                                if (argPtr1.getByteArray()[formatStart + formatLength] == '|') {
                                    break;
                                }
                            }
                            processSuccessfully = util.parseDateTime(aInt64, bytes0, start0, length0, bytes1,
                                    formatStart, formatLength, DateTimeParseMode.DATE_ONLY, false);
                            formatStart += formatLength + 1;
                        }
                        if (!processSuccessfully) {
                            throw new InvalidDataFormatException(sourceLoc, getIdentifier(),
                                    ATypeTag.SERIALIZED_DATE_TYPE_TAG);
                        }
                        aDate.setValue((int) (aInt64.getLongValue() / GregorianCalendarSystem.CHRONON_OF_DAY));
                        dateSerde.serialize(aDate, out);
                        result.set(resultStorage);
                    }
                };
            }

        };
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.functions.AbstractFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.PARSE_DATE;
    }
}
