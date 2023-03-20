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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.functions.AbstractScalarEval;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
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

abstract class AbstractUnixTimeEval extends AbstractScalarEval {

    private final IScalarEvaluator arg0;
    private final IScalarEvaluator arg1;

    private final IPointable argPtr0;
    private final IPointable argPtr1;
    private final UTF8StringPointable utf8Ptr;

    protected final GregorianCalendarSystem cal = GregorianCalendarSystem.getInstance();
    protected final TimezoneHelper tzHelper;

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    private final AMutableInt64 aInt64 = new AMutableInt64(0);
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput out = resultStorage.getDataOutput();

    AbstractUnixTimeEval(IScalarEvaluator arg0, SourceLocation sourceLoc, FunctionIdentifier functionIdentifier) {
        this(arg0, null, sourceLoc, functionIdentifier);
    }

    AbstractUnixTimeEval(IScalarEvaluator arg0, IScalarEvaluator arg1, SourceLocation sourceLoc,
            FunctionIdentifier fid) {
        super(sourceLoc, fid);
        this.arg0 = arg0;
        this.arg1 = arg1;
        this.argPtr0 = new VoidPointable();
        this.argPtr1 = arg1 != null ? new VoidPointable() : null;
        this.utf8Ptr = arg1 != null ? new UTF8StringPointable() : null;
        this.tzHelper = new TimezoneHelper(sourceLoc, fid);
    }

    private long getChronon(byte[] bytes, int offset, ATypeTag tag) {
        switch (tag) {
            case TIME:
                return ATimeSerializerDeserializer.getChronon(bytes, offset);
            case DATE:
                return ADateSerializerDeserializer.getChronon(bytes, offset);
            case DATETIME:
                return ADateTimeSerializerDeserializer.getChronon(bytes, offset);
        }
        return -1l;
    }

    protected ATypeTag tag = ATypeTag.NULL;
    protected Predicate<Byte> incorrectTag = i -> i != tag.serialize();

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        arg0.evaluate(tuple, argPtr0);
        if (arg1 != null) {
            arg1.evaluate(tuple, argPtr1);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
            return;
        }

        byte[] bytes0 = argPtr0.getByteArray();
        int offset0 = argPtr0.getStartOffset();
        if (incorrectTag.test(bytes0[offset0])) {
            throw new TypeMismatchException(srcLoc, funID, 0, bytes0[offset0], tag.serialize());
        }

        long chrononLocal = getChronon(bytes0, offset0 + 1, tag);
        long chrononUTC;
        if (arg1 != null) {
            byte[] bytes1 = argPtr1.getByteArray();
            int offset1 = argPtr1.getStartOffset();
            int len1 = argPtr1.getLength();
            if (bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                throw new TypeMismatchException(srcLoc, funID, 1, bytes1[offset1], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            }
            utf8Ptr.set(bytes1, offset1 + 1, len1 - 1);
            ZoneRules tzRules = tzHelper.parseTimeZone(utf8Ptr);
            LocalDateTime dt = toLocalDateTime(chrononLocal, cal);
            ZoneOffset tzOffset = tzRules.getOffset(dt);
            int tzOffsetMillis = (int) TimeUnit.SECONDS.toMillis(tzOffset.getTotalSeconds());
            chrononUTC = cal.adjustChrononByTimezone(chrononLocal, tzOffsetMillis);
        } else {
            chrononUTC = chrononLocal;
        }
        long unixTime = chrononToUnixTime(chrononUTC);

        resultStorage.reset();
        aInt64.setValue(unixTime);
        int64Serde.serialize(aInt64, out);
        result.set(resultStorage);
    }

    private static LocalDateTime toLocalDateTime(long chronon, GregorianCalendarSystem cal) {
        int year = cal.getYear(chronon);
        int month = cal.getMonthOfYear(chronon, year);
        int day = cal.getDayOfMonthYear(chronon, year, month);
        int hour = cal.getHourOfDay(chronon);
        int minute = cal.getMinOfHour(chronon);
        int second = cal.getSecOfMin(chronon);
        int milli = cal.getMillisOfSec(chronon);
        int nano = (int) TimeUnit.MILLISECONDS.toNanos(milli);
        return LocalDateTime.of(year, month, day, hour, minute, second, nano);
    }

    abstract long chrononToUnixTime(long chronon);
}
