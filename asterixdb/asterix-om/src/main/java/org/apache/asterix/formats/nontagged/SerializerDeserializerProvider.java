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
package org.apache.asterix.formats.nontagged;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.asterix.dataflow.data.nontagged.serde.ABinarySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AMissingSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APoint3DSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUUIDSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.ShortSerializerDeserializer;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class SerializerDeserializerProvider implements ISerializerDeserializerProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final SerializerDeserializerProvider INSTANCE = new SerializerDeserializerProvider();

    private SerializerDeserializerProvider() {
    }

    // Can't be shared among threads <Stateful>
    @SuppressWarnings("rawtypes")
    public ISerializerDeserializer getAStringSerializerDeserializer() {
        return addTag(new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader()));
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ISerializerDeserializer getSerializerDeserializer(Object typeInfo) {
        IAType type = (IAType) typeInfo;
        if (type == null) {
            return null;
        }
        switch (type.getTypeTag()) {
            case ANY:
            case UNION:
                // we could do smth better for nullable fields
                return AObjectSerializerDeserializer.INSTANCE;
            default:
                return addTag(getNonTaggedSerializerDeserializer(type));
        }
    }

    @SuppressWarnings("rawtypes")
    public ISerializerDeserializer getNonTaggedSerializerDeserializer(IAType type) {
        switch (type.getTypeTag()) {
            case CIRCLE:
                return ACircleSerializerDeserializer.INSTANCE;
            case DATE:
                return ADateSerializerDeserializer.INSTANCE;
            case DATETIME:
                return ADateTimeSerializerDeserializer.INSTANCE;
            case DOUBLE:
                return ADoubleSerializerDeserializer.INSTANCE;
            case FLOAT:
                return AFloatSerializerDeserializer.INSTANCE;
            case BOOLEAN:
                return ABooleanSerializerDeserializer.INSTANCE;
            case TINYINT:
                return AInt8SerializerDeserializer.INSTANCE;
            case SMALLINT:
                return AInt16SerializerDeserializer.INSTANCE;
            case INTEGER:
                return AInt32SerializerDeserializer.INSTANCE;
            case BIGINT:
                return AInt64SerializerDeserializer.INSTANCE;
            case LINE:
                return ALineSerializerDeserializer.INSTANCE;
            case MISSING:
                return AMissingSerializerDeserializer.INSTANCE;
            case NULL:
                return ANullSerializerDeserializer.INSTANCE;
            case STRING:
                return AStringSerializerDeserializer.INSTANCE;
            case BINARY:
                return ABinarySerializerDeserializer.INSTANCE;
            case TIME:
                return ATimeSerializerDeserializer.INSTANCE;
            case DURATION:
                return ADurationSerializerDeserializer.INSTANCE;
            case YEARMONTHDURATION:
                return AYearMonthDurationSerializerDeserializer.INSTANCE;
            case DAYTIMEDURATION:
                return ADayTimeDurationSerializerDeserializer.INSTANCE;
            case INTERVAL:
                return AIntervalSerializerDeserializer.INSTANCE;
            case ARRAY:
                return new AOrderedListSerializerDeserializer((AOrderedListType) type);
            case POINT:
                return APointSerializerDeserializer.INSTANCE;
            case POINT3D:
                return APoint3DSerializerDeserializer.INSTANCE;
            case RECTANGLE:
                return ARectangleSerializerDeserializer.INSTANCE;
            case POLYGON:
                return APolygonSerializerDeserializer.INSTANCE;
            case OBJECT:
                return new ARecordSerializerDeserializer((ARecordType) type);
            case MULTISET:
                return new AUnorderedListSerializerDeserializer((AUnorderedListType) type);
            case UUID:
                return AUUIDSerializerDeserializer.INSTANCE;
            case SHORTWITHOUTTYPEINFO:
                return ShortSerializerDeserializer.INSTANCE;
            case GEOMETRY:
                return AGeometrySerializerDeserializer.INSTANCE;
            default:
                throw new NotImplementedException(
                        "No serializer/deserializer implemented for type " + type.getTypeTag() + " .");
        }
    }

    @SuppressWarnings("rawtypes")
    public static ISerializerDeserializer addTag(final ISerializerDeserializer nonTaggedSerde) {
        return new ISerializerDeserializer<IAObject>() {

            private static final long serialVersionUID = 1L;

            @Override
            public IAObject deserialize(DataInput in) throws HyracksDataException {
                try {
                    ATypeTag tag = SerializerDeserializerUtil.deserializeTag(in);
                    //deserialize the tag (move input cursor forward) and check if it's not NULL tag
                    if (tag == ATypeTag.MISSING) {
                        return AMissing.MISSING;
                    }
                    if (tag == ATypeTag.NULL) {
                        return ANull.NULL;
                    }
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
                return (IAObject) nonTaggedSerde.deserialize(in);
            }

            @SuppressWarnings("unchecked")
            @Override
            public void serialize(IAObject instance, DataOutput out) throws HyracksDataException {
                SerializerDeserializerUtil.serializeTag(instance, out);
                nonTaggedSerde.serialize(instance, out);
            }
        };
    }
}
