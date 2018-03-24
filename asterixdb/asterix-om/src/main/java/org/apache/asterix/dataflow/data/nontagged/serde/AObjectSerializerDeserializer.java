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
package org.apache.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.ALine;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.APoint3D;
import org.apache.asterix.om.base.APolygon;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AObjectSerializerDeserializer implements ISerializerDeserializer<IAObject> {

    private static final long serialVersionUID = 1L;

    public static final AObjectSerializerDeserializer INSTANCE = new AObjectSerializerDeserializer();

    private AObjectSerializerDeserializer() {
    }

    @Override
    public IAObject deserialize(DataInput in) throws HyracksDataException {
        ATypeTag typeTag = SerializerDeserializerUtil.deserializeTag(in);
        switch (typeTag) {
            case MISSING:
                return AMissingSerializerDeserializer.INSTANCE.deserialize(in);
            case NULL:
                return ANullSerializerDeserializer.INSTANCE.deserialize(in);
            case BOOLEAN:
                return ABooleanSerializerDeserializer.INSTANCE.deserialize(in);
            case TINYINT:
                return AInt8SerializerDeserializer.INSTANCE.deserialize(in);
            case SMALLINT:
                return AInt16SerializerDeserializer.INSTANCE.deserialize(in);
            case INTEGER:
                return AInt32SerializerDeserializer.INSTANCE.deserialize(in);
            case BIGINT:
                return AInt64SerializerDeserializer.INSTANCE.deserialize(in);
            case FLOAT:
                return AFloatSerializerDeserializer.INSTANCE.deserialize(in);
            case DOUBLE:
                return ADoubleSerializerDeserializer.INSTANCE.deserialize(in);
            case STRING:
                return AStringSerializerDeserializer.INSTANCE.deserialize(in);
            case BINARY:
                return ABinarySerializerDeserializer.INSTANCE.deserialize(in);
            case DATE:
                return ADateSerializerDeserializer.INSTANCE.deserialize(in);
            case TIME:
                return ATimeSerializerDeserializer.INSTANCE.deserialize(in);
            case DATETIME:
                return ADateTimeSerializerDeserializer.INSTANCE.deserialize(in);
            case DURATION:
                return ADurationSerializerDeserializer.INSTANCE.deserialize(in);
            case YEARMONTHDURATION:
                return AYearMonthDurationSerializerDeserializer.INSTANCE.deserialize(in);
            case DAYTIMEDURATION:
                return ADayTimeDurationSerializerDeserializer.INSTANCE.deserialize(in);
            case INTERVAL:
                return AIntervalSerializerDeserializer.INSTANCE.deserialize(in);
            case POINT:
                return APointSerializerDeserializer.INSTANCE.deserialize(in);
            case POINT3D:
                return APoint3DSerializerDeserializer.INSTANCE.deserialize(in);
            case LINE:
                return ALineSerializerDeserializer.INSTANCE.deserialize(in);
            case RECTANGLE:
                return ARectangleSerializerDeserializer.INSTANCE.deserialize(in);
            case POLYGON:
                return APolygonSerializerDeserializer.INSTANCE.deserialize(in);
            case CIRCLE:
                return ACircleSerializerDeserializer.INSTANCE.deserialize(in);
            case OBJECT:
                return ARecordSerializerDeserializer.SCHEMALESS_INSTANCE.deserialize(in);
            case ARRAY:
                return AOrderedListSerializerDeserializer.SCHEMALESS_INSTANCE.deserialize(in);
            case MULTISET:
                return AUnorderedListSerializerDeserializer.SCHEMALESS_INSTANCE.deserialize(in);
            case GEOMETRY:
                return AGeometrySerializerDeserializer.INSTANCE.deserialize(in);
            default:
                throw new NotImplementedException("No serializer/deserializer implemented for type " + typeTag + " .");
        }
    }

    @Override
    public void serialize(IAObject instance, DataOutput out) throws HyracksDataException {
        IAType t = instance.getType();
        ATypeTag tag = t.getTypeTag();
        try {
            out.writeByte(tag.serialize());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        switch (tag) {
            case MISSING:
                AMissingSerializerDeserializer.INSTANCE.serialize((AMissing) instance, out);
                break;
            case NULL:
                ANullSerializerDeserializer.INSTANCE.serialize(instance, out);
                break;
            case BOOLEAN:
                ABooleanSerializerDeserializer.INSTANCE.serialize((ABoolean) instance, out);
                break;
            case TINYINT:
                AInt8SerializerDeserializer.INSTANCE.serialize((AInt8) instance, out);
                break;
            case SMALLINT:
                AInt16SerializerDeserializer.INSTANCE.serialize((AInt16) instance, out);
                break;
            case INTEGER:
                AInt32SerializerDeserializer.INSTANCE.serialize((AInt32) instance, out);
                break;
            case BIGINT:
                AInt64SerializerDeserializer.INSTANCE.serialize((AInt64) instance, out);
                break;
            case FLOAT:
                AFloatSerializerDeserializer.INSTANCE.serialize((AFloat) instance, out);
                break;
            case DOUBLE:
                ADoubleSerializerDeserializer.INSTANCE.serialize((ADouble) instance, out);
                break;
            case STRING:
                AStringSerializerDeserializer.INSTANCE.serialize((AString) instance, out);
                break;
            case BINARY:
                ABinarySerializerDeserializer.INSTANCE.serialize((ABinary) instance, out);
                break;
            case DATE:
                ADateSerializerDeserializer.INSTANCE.serialize((ADate) instance, out);
                break;
            case TIME:
                ATimeSerializerDeserializer.INSTANCE.serialize((ATime) instance, out);
                break;
            case DATETIME:
                ADateTimeSerializerDeserializer.INSTANCE.serialize((ADateTime) instance, out);
                break;
            case DURATION:
                ADurationSerializerDeserializer.INSTANCE.serialize((ADuration) instance, out);
                break;
            case INTERVAL:
                AIntervalSerializerDeserializer.INSTANCE.serialize((AInterval) instance, out);
                break;
            case POINT:
                APointSerializerDeserializer.INSTANCE.serialize((APoint) instance, out);
                break;
            case POINT3D:
                APoint3DSerializerDeserializer.INSTANCE.serialize((APoint3D) instance, out);
                break;
            case LINE:
                ALineSerializerDeserializer.INSTANCE.serialize((ALine) instance, out);
                break;
            case RECTANGLE:
                ARectangleSerializerDeserializer.INSTANCE.serialize((ARectangle) instance, out);
                break;
            case POLYGON:
                APolygonSerializerDeserializer.INSTANCE.serialize((APolygon) instance, out);
                break;
            case CIRCLE:
                ACircleSerializerDeserializer.INSTANCE.serialize((ACircle) instance, out);
                break;
            case OBJECT:
                ARecordSerializerDeserializer.SCHEMALESS_INSTANCE.serialize((ARecord) instance, out);
                break;
            case ARRAY:
                AOrderedListSerializerDeserializer.SCHEMALESS_INSTANCE.serialize((AOrderedList) instance, out);
                break;
            case MULTISET:
                AUnorderedListSerializerDeserializer.SCHEMALESS_INSTANCE.serialize((AUnorderedList) instance, out);
                break;
            case TYPE:
                ATypeSerializerDeserializer.INSTANCE.serialize((IAType) instance, out);
                break;
            case GEOMETRY:
                AGeometrySerializerDeserializer.INSTANCE.serialize((AGeometry) instance, out);
                break;
            default:
                throw new HyracksDataException(
                        "No serializer/deserializer implemented for type " + t.getTypeTag() + " .");
        }
    }
}
