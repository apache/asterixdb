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

import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.temporal.ATimeParserFactory;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem.Fields;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class AdjustDateTimeForTimeZoneDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private final static long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.ADJUST_DATETIME_FOR_TIMEZONE;
    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AdjustDateTimeForTimeZoneDescriptor();
        }
    };

    /* (non-Javadoc)
     * @see org.apache.asterix.runtime.base.IScalarFunctionDynamicDescriptor#createEvaluatorFactory(org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory[])
     */
    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable argPtr0 = new VoidPointable();
                    private IPointable argPtr1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    // possible output types
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);

                    private GregorianCalendarSystem calInstance = GregorianCalendarSystem.getInstance();

                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();
                    private final UTF8StringWriter utf8Writer = new UTF8StringWriter();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);

                        byte[] bytes0 = argPtr0.getByteArray();
                        int offset0 = argPtr0.getStartOffset();
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();
                        int len1 = argPtr1.getLength();

                        try {
                            if (bytes0[offset0] == ATypeTag.SERIALIZED_NULL_TYPE_TAG
                                    || bytes1[offset1] == ATypeTag.SERIALIZED_NULL_TYPE_TAG) {
                                nullSerde.serialize(ANull.NULL, out);
                                result.set(resultStorage);
                                return;
                            }

                            if (bytes0[offset0] != ATypeTag.SERIALIZED_DATETIME_TYPE_TAG) {
                                throw new AlgebricksException(
                                        FID.getName() + ": expects type DATETIME/NULL for parameter 0 but got "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes0[offset0]));
                            }

                            if (bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                throw new AlgebricksException(
                                        FID.getName() + ": expects type STRING/NULL for parameter 1 but got "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]));
                            }

                            utf8Ptr.set(bytes1, offset1 + 1, len1 - 1);
                            int timezone = ATimeParserFactory.parseTimezonePart(utf8Ptr.getByteArray(),
                                    utf8Ptr.getCharStartOffset());

                            if (!calInstance.validateTimeZone(timezone)) {
                                throw new AlgebricksException(FID.getName() + ": wrong format for a time zone string!");
                            }

                            long chronon = ADateTimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);

                            chronon = calInstance.adjustChrononByTimezone(chronon, timezone);

                            StringBuilder sbder = new StringBuilder();
                            calInstance.getExtendStringRepUntilField(chronon, timezone, sbder, Fields.YEAR,
                                    Fields.MILLISECOND, true);

                            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            utf8Writer.writeUTF8(sbder.toString(), out);
                        } catch (Exception e1) {
                            throw new AlgebricksException(e1);
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
