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
package edu.uci.ics.asterix.formats.nontagged;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APoint3DSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AYearMonthDurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AqlSerializerDeserializerProvider implements ISerializerDeserializerProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final AqlSerializerDeserializerProvider INSTANCE = new AqlSerializerDeserializerProvider();

    private AqlSerializerDeserializerProvider() {
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ISerializerDeserializer getSerializerDeserializer(Object typeInfo) {
        IAType aqlType = (IAType) typeInfo;
        switch (aqlType.getTypeTag()) {
            case ANY:
            case UNION: { // we could do smth better for nullable fields
                return AObjectSerializerDeserializer.INSTANCE;
            }
            default: {
                return addTag(getNonTaggedSerializerDeserializer(aqlType), aqlType.getTypeTag());
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public ISerializerDeserializer getNonTaggedSerializerDeserializer(IAType aqlType) {
        switch (aqlType.getTypeTag()) {
            case CIRCLE: {
                return ACircleSerializerDeserializer.INSTANCE;
            }
            case DATE: {
                return ADateSerializerDeserializer.INSTANCE;
            }
            case DATETIME: {
                return ADateTimeSerializerDeserializer.INSTANCE;
            }
            case DOUBLE: {
                return ADoubleSerializerDeserializer.INSTANCE;
            }
            case FLOAT: {
                return AFloatSerializerDeserializer.INSTANCE;
            }
            case BOOLEAN: {
                return ABooleanSerializerDeserializer.INSTANCE;
            }
            case INT8: {
                return AInt8SerializerDeserializer.INSTANCE;
            }
            case INT16: {
                return AInt16SerializerDeserializer.INSTANCE;
            }
            case INT32: {
                return AInt32SerializerDeserializer.INSTANCE;
            }
            case INT64: {
                return AInt64SerializerDeserializer.INSTANCE;
            }
            case LINE: {
                return ALineSerializerDeserializer.INSTANCE;
            }
            case NULL: {
                return ANullSerializerDeserializer.INSTANCE;
            }
            case STRING: {
                return AStringSerializerDeserializer.INSTANCE;
            }
            case TIME: {
                return ATimeSerializerDeserializer.INSTANCE;
            }
            case DURATION: {
                return ADurationSerializerDeserializer.INSTANCE;
            }
            case YEARMONTHDURATION: {
                return AYearMonthDurationSerializerDeserializer.INSTANCE;
            }
            case DAYTIMEDURATION: {
                return ADayTimeDurationSerializerDeserializer.INSTANCE;
            }
            case INTERVAL: {
                return AIntervalSerializerDeserializer.INSTANCE;
            }
            case ORDEREDLIST: {
                return new AOrderedListSerializerDeserializer((AOrderedListType) aqlType);
            }
            case POINT: {
                return APointSerializerDeserializer.INSTANCE;
            }
            case POINT3D: {
                return APoint3DSerializerDeserializer.INSTANCE;
            }
            case RECTANGLE: {
                return ARectangleSerializerDeserializer.INSTANCE;
            }
            case POLYGON: {
                return APolygonSerializerDeserializer.INSTANCE;
            }
            case RECORD: {
                return new ARecordSerializerDeserializer((ARecordType) aqlType);
            }
            case UNORDEREDLIST: {
                return new AUnorderedListSerializerDeserializer((AUnorderedListType) aqlType);
            }
            default: {
                throw new NotImplementedException("No serializer/deserializer implemented for type "
                        + aqlType.getTypeTag() + " .");
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private ISerializerDeserializer addTag(final ISerializerDeserializer nonTaggedSerde, final ATypeTag typeTag) {
        return new ISerializerDeserializer<IAObject>() {

            private static final long serialVersionUID = 1L;

            @Override
            public IAObject deserialize(DataInput in) throws HyracksDataException {
                try {
                    //deserialize the tag to move the  input cursor forward 
                    SerializerDeserializerUtil.deserializeTag(in);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
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
