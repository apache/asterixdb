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
import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.om.base.temporal.ATimeParserFactory;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem.Fields;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringWriter;

@MissingNullInOutFunction
public class AdjustTimeForTimeZoneDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private final static long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = BuiltinFunctions.ADJUST_TIME_FOR_TIMEZONE;
    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AdjustTimeForTimeZoneDescriptor();
        }
    };

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.base.IScalarFunctionDynamicDescriptor#createEvaluatorFactory(org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory[])
     */
    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable argPtr0 = new VoidPointable();
                    private IPointable argPtr1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    private GregorianCalendarSystem calInstance = GregorianCalendarSystem.getInstance();
                    private final UTF8StringWriter writer = new UTF8StringWriter();
                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

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
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();
                        int len1 = argPtr1.getLength();

                        try {
                            if (bytes0[offset0] != ATypeTag.SERIALIZED_TIME_TYPE_TAG) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, bytes0[offset0],
                                        ATypeTag.SERIALIZED_TIME_TYPE_TAG);
                            }

                            if (bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, bytes1[offset1],
                                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            }

                            utf8Ptr.set(argPtr1.getByteArray(), offset1 + 1, len1 - 1);
                            int timezone = ATimeParserFactory.parseTimezonePart(bytes1, utf8Ptr.getCharStartOffset());

                            if (!calInstance.validateTimeZone(timezone)) {
                                throw new InvalidDataFormatException(sourceLoc, getIdentifier(), "timezone");
                            }

                            int chronon = ATimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);

                            chronon = (int) calInstance.adjustChrononByTimezone(chronon, timezone);

                            StringBuilder sbder = new StringBuilder();

                            calInstance.getExtendStringRepUntilField(chronon, timezone, sbder, Fields.HOUR,
                                    Fields.MILLISECOND, true);

                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            writer.writeUTF8(sbder, out);
                        } catch (IOException e1) {
                            throw HyracksDataException.create(e1);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.functions.IFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
