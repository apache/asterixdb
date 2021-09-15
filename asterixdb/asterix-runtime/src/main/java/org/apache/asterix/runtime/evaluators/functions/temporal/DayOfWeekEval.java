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
import java.util.Objects;

import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.temporal.DateTimeFormatUtils;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class DayOfWeekEval implements IScalarEvaluator {

    private final IScalarEvaluator eval0;
    private final IScalarEvaluator eval1;
    private final IPointable arg0Ptr = new VoidPointable();
    private final IPointable arg1Ptr;
    private final UTF8StringPointable str1Ptr;

    private final GregorianCalendarSystem calSystem = GregorianCalendarSystem.getInstance();

    // possible returning types
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    private final AMutableInt64 aInt64 = new AMutableInt64(0);

    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput out = resultStorage.getDataOutput();

    private final FunctionIdentifier fid;
    private final SourceLocation sourceLoc;

    public DayOfWeekEval(IScalarEvaluator eval0, IScalarEvaluator eval1, FunctionIdentifier fid,
            SourceLocation sourceLoc) {
        this.eval0 = Objects.requireNonNull(eval0);
        this.eval1 = eval1;
        arg1Ptr = eval1 != null ? new VoidPointable() : null;
        str1Ptr = eval1 != null ? new UTF8StringPointable() : null;
        this.fid = Objects.requireNonNull(fid);
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        eval0.evaluate(tuple, arg0Ptr);
        if (eval1 != null) {
            eval1.evaluate(tuple, arg1Ptr);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, arg0Ptr, arg1Ptr)) {
            return;
        }

        byte[] bytes0 = arg0Ptr.getByteArray();
        int offset0 = arg0Ptr.getStartOffset();

        long chronon;
        if (bytes0[offset0] == ATypeTag.SERIALIZED_DATETIME_TYPE_TAG) {
            chronon = ADateTimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);
        } else if (bytes0[offset0] == ATypeTag.SERIALIZED_DATE_TYPE_TAG) {
            chronon = ADateSerializerDeserializer.getChronon(bytes0, offset0 + 1)
                    * GregorianCalendarSystem.CHRONON_OF_DAY;
        } else {
            throw new TypeMismatchException(sourceLoc, fid, 0, bytes0[offset0], ATypeTag.SERIALIZED_DATETIME_TYPE_TAG,
                    ATypeTag.SERIALIZED_DATE_TYPE_TAG);
        }

        int weekStart = 0;
        if (eval1 != null) {
            byte[] bytes1 = arg1Ptr.getByteArray();
            int offset1 = arg1Ptr.getStartOffset();
            ATypeTag tt1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes1[offset1]);
            switch (tt1) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                    int v = ATypeHierarchy.getIntegerValue(fid.getName(), 1, bytes1, offset1);
                    weekStart = v - 1;
                    break;
                case STRING:
                    int len1 = arg1Ptr.getLength();
                    str1Ptr.set(bytes1, offset1 + 1, len1 - 1);
                    int str1Len = str1Ptr.getStringLength();
                    weekStart = DateTimeFormatUtils.weekdayIDSearch(str1Ptr.getByteArray(),
                            str1Ptr.getCharStartOffset(), str1Len, str1Len == 3);
                    break;
                default:
                    throw new TypeMismatchException(sourceLoc, fid, 1, bytes1[offset1],
                            ATypeTag.SERIALIZED_INT8_TYPE_TAG, ATypeTag.SERIALIZED_INT16_TYPE_TAG,
                            ATypeTag.SERIALIZED_INT32_TYPE_TAG, ATypeTag.SERIALIZED_INT64_TYPE_TAG,
                            ATypeTag.SERIALIZED_FLOAT_TYPE_TAG, ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG,
                            ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            }
            boolean weekStartValid = weekStart >= 0 && weekStart < GregorianCalendarSystem.DAYS_IN_A_WEEK;
            if (!weekStartValid) {
                throw new InvalidDataFormatException(sourceLoc, fid, "week_start_day");
            }
        }

        int dowRaw = calSystem.getDayOfWeek(chronon);
        int dowRawOffset = dowRaw - weekStart;
        int dow0 = dowRawOffset < 0 ? dowRawOffset + GregorianCalendarSystem.DAYS_IN_A_WEEK : dowRawOffset;
        int dow1 = dow0 + 1;

        resultStorage.reset();
        aInt64.setValue(dow1);
        int64Serde.serialize(aInt64, out);
        result.set(resultStorage);
    }
}
