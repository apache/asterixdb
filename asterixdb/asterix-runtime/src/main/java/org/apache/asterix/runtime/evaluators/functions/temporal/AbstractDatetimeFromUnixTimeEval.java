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
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.AMutableDateTime;
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

abstract class AbstractDatetimeFromUnixTimeEval extends AbstractScalarEval {

    private final IScalarEvaluator arg0;
    private final IScalarEvaluator arg1;

    private final IPointable argPtr0;
    private final IPointable argPtr1;
    private final UTF8StringPointable utf8Ptr;

    protected final GregorianCalendarSystem cal = GregorianCalendarSystem.getInstance();
    protected final TimezoneHelper tzHelper;

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ADateTime> datetimeSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADATETIME);
    private final AMutableDateTime aDatetime = new AMutableDateTime(0);
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final DataOutput out = resultStorage.getDataOutput();

    AbstractDatetimeFromUnixTimeEval(IScalarEvaluator arg0, SourceLocation sourceLoc, FunctionIdentifier fid) {
        this(arg0, null, sourceLoc, fid);
    }

    AbstractDatetimeFromUnixTimeEval(IScalarEvaluator arg0, IScalarEvaluator arg1, SourceLocation sourceLoc,
            FunctionIdentifier fid) {
        super(sourceLoc, fid);
        this.arg0 = arg0;
        this.arg1 = arg1;
        this.argPtr0 = new VoidPointable();
        this.argPtr1 = arg1 != null ? new VoidPointable() : null;
        this.utf8Ptr = arg1 != null ? new UTF8StringPointable() : null;
        this.tzHelper = new TimezoneHelper(sourceLoc, fid);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        arg0.evaluate(tuple, argPtr0);
        if (arg1 != null) {
            arg1.evaluate(tuple, argPtr1);
        }

        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
            return;
        }

        long unixTime;
        byte[] bytes = argPtr0.getByteArray();
        int offset = argPtr0.getStartOffset();
        ATypeTag argPtrTypeTag = ATypeTag.VALUE_TYPE_MAPPING[bytes[offset]];
        switch (argPtrTypeTag) {
            case TINYINT:
                unixTime = AInt8SerializerDeserializer.getByte(bytes, offset + 1);
                break;
            case SMALLINT:
                unixTime = AInt16SerializerDeserializer.getShort(bytes, offset + 1);
                break;
            case INTEGER:
                unixTime = AInt32SerializerDeserializer.getInt(bytes, offset + 1);
                break;
            case BIGINT:
                unixTime = AInt64SerializerDeserializer.getLong(bytes, offset + 1);
                break;
            default:
                throw new TypeMismatchException(srcLoc, funID, 0, bytes[offset], ATypeTag.SERIALIZED_INT8_TYPE_TAG,
                        ATypeTag.SERIALIZED_INT16_TYPE_TAG, ATypeTag.SERIALIZED_INT32_TYPE_TAG,
                        ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        }

        long chrononUTC = chrononFromUnixTime(unixTime);
        long chrononAdjusted;

        if (arg1 != null) {
            byte[] bytes1 = argPtr1.getByteArray();
            int offset1 = argPtr1.getStartOffset();
            int len1 = argPtr1.getLength();
            if (bytes1[offset1] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                throw new TypeMismatchException(srcLoc, funID, 1, bytes1[offset1], ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            }
            utf8Ptr.set(bytes1, offset1 + 1, len1 - 1);
            ZoneRules tzRules = tzHelper.parseTimeZone(utf8Ptr);
            ZoneOffset tzOffset = tzRules.getOffset(Instant.ofEpochMilli(chrononUTC));
            int tzOffsetMillis = (int) TimeUnit.SECONDS.toMillis(tzOffset.getTotalSeconds());
            chrononAdjusted = cal.adjustChrononByTimezone(chrononUTC, -tzOffsetMillis);
        } else {
            chrononAdjusted = chrononUTC;
        }

        resultStorage.reset();
        aDatetime.setValue(chrononAdjusted);
        datetimeSerde.serialize(aDatetime, out);
        result.set(resultStorage);
    }

    protected abstract long chrononFromUnixTime(long argValue);
}
