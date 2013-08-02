/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.dataflow.data.nontagged.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ACircle;
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.ADuration;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt16;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AInt8;
import edu.uci.ics.asterix.om.base.AInterval;
import edu.uci.ics.asterix.om.base.ALine;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.APoint;
import edu.uci.ics.asterix.om.base.APoint3D;
import edu.uci.ics.asterix.om.base.APolygon;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.ARectangle;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.AUnorderedList;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AObjectSerializerDeserializer implements ISerializerDeserializer<IAObject> {

    private static final long serialVersionUID = 1L;

    public static final AObjectSerializerDeserializer INSTANCE = new AObjectSerializerDeserializer();

    private AObjectSerializerDeserializer() {
    }

    @Override
    public IAObject deserialize(DataInput in) throws HyracksDataException {
        ATypeTag typeTag = SerializerDeserializerUtil.deserializeTag(in);
        switch (typeTag) {
            case NULL: {
                return ANullSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case BOOLEAN: {
                return ABooleanSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case INT8: {
                return AInt8SerializerDeserializer.INSTANCE.deserialize(in);
            }
            case INT16: {
                return AInt16SerializerDeserializer.INSTANCE.deserialize(in);
            }
            case INT32: {
                return AInt32SerializerDeserializer.INSTANCE.deserialize(in);
            }
            case INT64: {
                return AInt64SerializerDeserializer.INSTANCE.deserialize(in);
            }
            case FLOAT: {
                return AFloatSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case DOUBLE: {
                return ADoubleSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case STRING: {
                return AStringSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case DATE: {
                return ADateSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case TIME: {
                return ATimeSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case DATETIME: {
                return ADateTimeSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case DURATION: {
                return ADurationSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case YEARMONTHDURATION: {
                return AYearMonthDurationSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case DAYTIMEDURATION: {
                return ADayTimeDurationSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case INTERVAL: {
                return AIntervalSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case POINT: {
                return APointSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case POINT3D: {
                return APoint3DSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case LINE: {
                return ALineSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case RECTANGLE: {
                return ARectangleSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case POLYGON: {
                return APolygonSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case CIRCLE: {
                return ACircleSerializerDeserializer.INSTANCE.deserialize(in);
            }
            case RECORD: {
                return ARecordSerializerDeserializer.SCHEMALESS_INSTANCE.deserialize(in);
            }
            case ORDEREDLIST: {
                return AOrderedListSerializerDeserializer.SCHEMALESS_INSTANCE.deserialize(in);
            }
            case UNORDEREDLIST: {
                return AUnorderedListSerializerDeserializer.SCHEMALESS_INSTANCE.deserialize(in);
            }
            // case TYPE: {
            // return AUnorderedListBytesConverter.INSTANCE.deserialize(in);
            // }
            default: {
                throw new NotImplementedException("No serializer/deserializer implemented for type " + typeTag + " .");
            }
        }
    }

    @Override
    public void serialize(IAObject instance, DataOutput out) throws HyracksDataException {
        IAType t = instance.getType();
        ATypeTag tag = t.getTypeTag();
        try {
            out.writeByte(tag.serialize());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        switch (tag) {
            case NULL: {
                ANullSerializerDeserializer.INSTANCE.serialize((ANull) instance, out);
                break;
            }
            case BOOLEAN: {
                ABooleanSerializerDeserializer.INSTANCE.serialize((ABoolean) instance, out);
                break;
            }
            case INT8: {
                AInt8SerializerDeserializer.INSTANCE.serialize((AInt8) instance, out);
                break;
            }
            case INT16: {
                AInt16SerializerDeserializer.INSTANCE.serialize((AInt16) instance, out);
                break;
            }
            case INT32: {
                AInt32SerializerDeserializer.INSTANCE.serialize((AInt32) instance, out);
                break;
            }
            case INT64: {
                AInt64SerializerDeserializer.INSTANCE.serialize((AInt64) instance, out);
                break;
            }
            case FLOAT: {
                AFloatSerializerDeserializer.INSTANCE.serialize((AFloat) instance, out);
                break;
            }
            case DOUBLE: {
                ADoubleSerializerDeserializer.INSTANCE.serialize((ADouble) instance, out);
                break;
            }
            case STRING: {
                AStringSerializerDeserializer.INSTANCE.serialize((AString) instance, out);
                break;
            }
            case DATE: {
                ADateSerializerDeserializer.INSTANCE.serialize((ADate) instance, out);
                break;
            }
            case TIME: {
                ATimeSerializerDeserializer.INSTANCE.serialize((ATime) instance, out);
                break;
            }
            case DATETIME: {
                ADateTimeSerializerDeserializer.INSTANCE.serialize((ADateTime) instance, out);
                break;
            }
            case DURATION: {
                ADurationSerializerDeserializer.INSTANCE.serialize((ADuration) instance, out);
                break;
            }
            case INTERVAL: {
                AIntervalSerializerDeserializer.INSTANCE.serialize((AInterval) instance, out);
                break;
            }
            case POINT: {
                APointSerializerDeserializer.INSTANCE.serialize((APoint) instance, out);
                break;
            }
            case POINT3D: {
                APoint3DSerializerDeserializer.INSTANCE.serialize((APoint3D) instance, out);
                break;
            }
            case LINE: {
                ALineSerializerDeserializer.INSTANCE.serialize((ALine) instance, out);
                break;
            }
            case RECTANGLE: {
                ARectangleSerializerDeserializer.INSTANCE.serialize((ARectangle) instance, out);
                break;
            }
            case POLYGON: {
                APolygonSerializerDeserializer.INSTANCE.serialize((APolygon) instance, out);
                break;
            }
            case CIRCLE: {
                ACircleSerializerDeserializer.INSTANCE.serialize((ACircle) instance, out);
                break;
            }
            case RECORD: {
                ARecordSerializerDeserializer.SCHEMALESS_INSTANCE.serialize((ARecord) instance, out);
                break;
            }
            case ORDEREDLIST: {
                AOrderedListSerializerDeserializer.SCHEMALESS_INSTANCE.serialize((AOrderedList) instance, out);
            }
            case UNORDEREDLIST: {
                AUnorderedListSerializerDeserializer.SCHEMALESS_INSTANCE.serialize((AUnorderedList) instance, out);
            }
            case TYPE: {
                ATypeSerializerDeserializer.INSTANCE.serialize((IAType) instance, out);
            }
            default: {
                throw new NotImplementedException("No serializer/deserializer implemented for type " + t.getTypeTag()
                        + " .");
            }
        }
    }
}
