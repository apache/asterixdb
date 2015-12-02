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
package org.apache.asterix.external.library.java;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APoint3DSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AMutableCircle;
import org.apache.asterix.om.base.AMutableDate;
import org.apache.asterix.om.base.AMutableDateTime;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.AMutableFloat;
import org.apache.asterix.om.base.AMutableInt16;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableInt8;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.base.AMutableLine;
import org.apache.asterix.om.base.AMutableOrderedList;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutablePoint3D;
import org.apache.asterix.om.base.AMutablePolygon;
import org.apache.asterix.om.base.AMutableRecord;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.AMutableString;
import org.apache.asterix.om.base.AMutableTime;
import org.apache.asterix.om.base.AMutableUnorderedList;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class JObjects {

    public static abstract class JObject implements IJObject {

        protected IAObject value;
        protected byte[] bytes;

        protected JObject(IAObject value) {
            this.value = value;
        }

        @Override
        public ATypeTag getTypeTag() {
            return value.getType().getTypeTag();
        }

        @Override
        public IAObject getIAObject() {
            return value;
        }

    }

    /*
     *  This class is necessary to be able to serialize null objects
      *  in cases of setting "null" results
     *
     *
     */
    public static class JNull implements IJObject {
        public final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();

        public final static JNull INSTANCE = new JNull();

        private JNull() {
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.NULL;
        }

        @Override
        public IAObject getIAObject() {
            return ANull.NULL;
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(SER_NULL_TYPE_TAG);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        }

        @Override
        public void reset() {
        }

    }

    public static final class JByte extends JObject {

        public JByte(byte value) {
            super(new AMutableInt8(value));
        }

        public void setValue(byte v) {
            ((AMutableInt8) value).setValue(v);
        }

        public byte getValue() {
            return ((AMutableInt8) value).getByteValue();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            AInt8SerializerDeserializer.INSTANCE.serialize((AInt8) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableInt8) value).setValue((byte) 0);
        }
    }

    public static final class JShort extends JObject {

        private AMutableInt16 value;

        public JShort(short value) {
            super(new AMutableInt16(value));
        }

        public void setValue(byte v) {
            value.setValue(v);
        }

        public short getValue() {
            return value.getShortValue();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            AInt16SerializerDeserializer.INSTANCE.serialize(value, dataOutput);
        }

        @Override
        public void reset() {
            value.setValue((short) 0);
        }

    }

    public static final class JInt extends JObject {

        public JInt(int value) {
            super(new AMutableInt32(value));
        }

        public void setValue(int v) {
            ((AMutableInt32) value).setValue(v);
        }

        public int getValue() {
            return ((AMutableInt32) value).getIntegerValue().intValue();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            AInt32SerializerDeserializer.INSTANCE.serialize((AInt32) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableInt32) value).setValue(0);
        }
    }

    public static final class JBoolean implements IJObject {

        private boolean value;

        public JBoolean(boolean value) {
            this.value = value;
        }

        public void setValue(boolean value) {
            this.value = value;
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.BOOLEAN;
        }

        @Override
        public IAObject getIAObject() {
            return value ? ABoolean.TRUE : ABoolean.FALSE;
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.BOOLEAN.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ABooleanSerializerDeserializer.INSTANCE.serialize((ABoolean) getIAObject(), dataOutput);
        }

        @Override
        public void reset() {
            value = false;
        }

    }

    public static final class JLong extends JObject {

        public JLong(long v) {
            super(new AMutableInt64(v));
        }

        public void setValue(long v) {
            ((AMutableInt64) value).setValue(v);
        }

        public long getValue() {
            return ((AMutableInt64) value).getLongValue();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            AInt64SerializerDeserializer.INSTANCE.serialize((AInt64) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableInt64) value).setValue(0);
        }

    }

    public static final class JDouble extends JObject {

        public JDouble(double v) {
            super(new AMutableDouble(v));
        }

        public void setValue(double v) {
            ((AMutableDouble) value).setValue(v);
        }

        public double getValue() {
            return ((AMutableDouble) value).getDoubleValue();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ADoubleSerializerDeserializer.INSTANCE.serialize((ADouble) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableDouble) value).setValue(0);
        }

    }

    public static final class JString extends JObject {

        private final AStringSerializerDeserializer aStringSerDer = AStringSerializerDeserializer.INSTANCE;

        public JString(String v) {
            super(new AMutableString(v));
        }

        public void setValue(String v) {
            ((AMutableString) value).setValue(v);
        }

        public String getValue() {
            return ((AMutableString) value).getStringValue();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            aStringSerDer.serialize((AString) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableString) value).setValue("");
        }

    }

    public static final class JFloat extends JObject {

        public JFloat(float v) {
            super(new AMutableFloat(v));
        }

        public void setValue(float v) {
            ((AMutableFloat) value).setValue(v);
        }

        public float getValue() {
            return ((AMutableFloat) value).getFloatValue();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            AFloatSerializerDeserializer.INSTANCE.serialize((AFloat) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableFloat) value).setValue(0);
        }

    }

    public static final class JPoint extends JObject {

        public JPoint(double x, double y) {
            super(new AMutablePoint(x, y));
        }

        public void setValue(double x, double y) {
            ((AMutablePoint) value).setValue(x, y);
        }

        public double getXValue() {
            return ((AMutablePoint) value).getX();
        }

        public double getYValue() {
            return ((AMutablePoint) value).getY();
        }

        public IAObject getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value.toString();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            APointSerializerDeserializer.INSTANCE.serialize((APoint) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutablePoint) value).setValue(0, 0);
        }
    }

    public static final class JRectangle extends JObject {

        public JRectangle(JPoint p1, JPoint p2) {
            super(new AMutableRectangle((APoint) p1.getIAObject(), (APoint) p2.getIAObject()));
        }

        public void setValue(JPoint p1, JPoint p2) {
            ((AMutableRectangle) value).setValue((APoint) p1.getValue(), (APoint) p2.getValue());
        }

        public void setValue(APoint p1, APoint p2) {
            ((AMutableRectangle) value).setValue(p1, p2);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.RECTANGLE;
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(value.getType().getTypeTag().serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ARectangleSerializerDeserializer.INSTANCE.serialize((ARectangle) value, dataOutput);
        }

        @Override
        public void reset() {
        }

    }

    public static final class JTime extends JObject {

        public JTime(int timeInMillsec) {
            super(new AMutableTime(timeInMillsec));
        }

        public void setValue(int timeInMillsec) {
            ((AMutableTime) value).setValue(timeInMillsec);
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.TIME.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ATimeSerializerDeserializer.INSTANCE.serialize((AMutableTime) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableTime) value).setValue(0);
        }

    }

    public static final class JInterval extends JObject {

        public JInterval(long intervalStart, long intervalEnd) {
            super(new AMutableInterval(intervalStart, intervalEnd, (byte) 0));
        }

        public void setValue(long intervalStart, long intervalEnd, byte typetag) throws AlgebricksException {
            ((AMutableInterval) value).setValue(intervalStart, intervalEnd, typetag);
        }

        public long getIntervalStart() {
            return ((AMutableInterval) value).getIntervalStart();
        }

        public long getIntervalEnd() {
            return ((AMutableInterval) value).getIntervalEnd();
        }

        public short getIntervalType() {
            return ((AMutableInterval) value).getIntervalType();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.INTERVAL.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            AIntervalSerializerDeserializer.INSTANCE.serialize(((AMutableInterval) value), dataOutput);
        }

        @Override
        public void reset() throws AlgebricksException {
            ((AMutableInterval) value).setValue(0L, 0L, (byte) 0);
        }

    }

    public static final class JDate extends JObject {

        public JDate(int chrononTimeInDays) {
            super(new AMutableDate(chrononTimeInDays));
        }

        public void setValue(int chrononTimeInDays) {
            ((AMutableDate) value).setValue(chrononTimeInDays);
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.DATE.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ADateSerializerDeserializer.INSTANCE.serialize(((AMutableDate) value), dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableDate) value).setValue(0);
        }
    }

    public static final class JDateTime extends JObject {

        public JDateTime(long chrononTime) {
            super(new AMutableDateTime(chrononTime));
        }

        public void setValue(long chrononTime) {
            ((AMutableDateTime) value).setValue(chrononTime);
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.DATETIME.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ADateTimeSerializerDeserializer.INSTANCE.serialize(((AMutableDateTime) value), dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableDateTime) value).setValue(0);
        }

    }

    public static final class JDuration extends JObject {

        public JDuration(int months, long milliseconds) {
            super(new AMutableDuration(months, milliseconds));
        }

        public void setValue(int months, long milliseconds) {
            ((AMutableDuration) value).setValue(months, milliseconds);
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.DURATION.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ADurationSerializerDeserializer.INSTANCE.serialize(((AMutableDuration) value), dataOutput);
        }

        @Override
        public void reset() {
            ((AMutableDuration) value).setValue(0, 0);
        }

    }

    public static final class JPolygon extends JObject {

        public JPolygon(JPoint[] points) {
            super(new AMutablePolygon(getAPoints(points)));
        }

        public void setValue(APoint[] points) {
            ((AMutablePolygon) value).setValue(points);
        }

        public void setValue(JPoint[] points) {
            ((AMutablePolygon) value).setValue(getAPoints(points));
        }

        private static APoint[] getAPoints(JPoint[] jpoints) {
            APoint[] apoints = new APoint[jpoints.length];
            int index = 0;
            for (JPoint jpoint : jpoints) {
                apoints[index++] = (APoint) jpoint.getIAObject();
            }
            return apoints;
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.POLYGON;
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.POLYGON.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            APolygonSerializerDeserializer.INSTANCE.serialize((AMutablePolygon) value, dataOutput);
        }

        @Override
        public void reset() {
            ((AMutablePolygon) value).setValue(null);
        }

    }

    public static final class JCircle extends JObject {

        public JCircle(JPoint center, double radius) {
            super(new AMutableCircle((APoint) center.getIAObject(), radius));
        }

        public void setValue(JPoint center, double radius) {
            ((AMutableCircle) (value)).setValue((APoint) center.getIAObject(), radius);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.CIRCLE;
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.CIRCLE.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ACircleSerializerDeserializer.INSTANCE.serialize(((AMutableCircle) (value)), dataOutput);
        }

        @Override
        public void reset() {
        }
    }

    public static final class JLine extends JObject {

        public JLine(JPoint p1, JPoint p2) {
            super(new AMutableLine((APoint) p1.getIAObject(), (APoint) p2.getIAObject()));
        }

        public void setValue(JPoint p1, JPoint p2) {
            ((AMutableLine) value).setValue((APoint) p1.getIAObject(), (APoint) p2.getIAObject());
        }

        public void setValue(APoint p1, APoint p2) {
            ((AMutableLine) value).setValue(p1, p2);
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.LINE.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            ALineSerializerDeserializer.INSTANCE.serialize(((AMutableLine) value), dataOutput);
        }

        @Override
        public void reset() {
            // TODO Auto-generated method stub

        }

    }

    public static final class JPoint3D extends JObject {

        public JPoint3D(double x, double y, double z) {
            super(new AMutablePoint3D(x, y, z));
        }

        public void setValue(double x, double y, double z) {
            ((AMutablePoint3D) value).setValue(x, y, z);
        }

        public double getXValue() {
            return ((AMutablePoint3D) value).getX();
        }

        public double getYValue() {
            return ((AMutablePoint3D) value).getY();
        }

        public double getZValue() {
            return ((AMutablePoint3D) value).getZ();
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            if (writeTypeTag) {
                try {
                    dataOutput.writeByte(ATypeTag.POINT3D.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            APoint3DSerializerDeserializer.INSTANCE.serialize(((AMutablePoint3D) value), dataOutput);
        }

        @Override
        public void reset() {
            // TODO Auto-generated method stub

        }
    }

    public static abstract class JList implements IJObject {
        protected List<IJObject> jObjects;

        public JList() {
            jObjects = new ArrayList<IJObject>();
        }

        public boolean isEmpty() {
            return jObjects.isEmpty();
        }

        public void add(IJObject jObject) {
            jObjects.add(jObject);
        }

        public void addAll(Collection<IJObject> jObjectCollection) {
            jObjects.addAll(jObjectCollection);
        }

        public void clear() {
            jObjects.clear();
        }

        public IJObject getElement(int index) {
            return jObjects.get(index);
        }

        public int size() {
            return jObjects.size();
        }

        public Iterator<IJObject> iterator() {
            return jObjects.iterator();
        }
    }

    public static final class JOrderedList extends JList {

        private AOrderedListType listType;

        public JOrderedList(IJObject jObject) {
            super();
            this.listType = new AOrderedListType(jObject.getIAObject().getType(), null);
        }

        public JOrderedList(IAType listItemType) {
            super();
            this.listType = new AOrderedListType(listItemType, null);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.ORDEREDLIST;
        }

        @Override
        public IAObject getIAObject() {
            AMutableOrderedList v = new AMutableOrderedList(listType);
            for (IJObject jObj : jObjects) {
                v.add(jObj.getIAObject());
            }
            return v;
        }

        public AOrderedListType getListType() {
            return listType;
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            IAsterixListBuilder listBuilder = new UnorderedListBuilder();
            listBuilder.reset(listType);
            ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
            for (IJObject jObject : jObjects) {
                fieldValue.reset();
                jObject.serialize(fieldValue.getDataOutput(), true);
                listBuilder.addItem(fieldValue);
            }
            listBuilder.write(dataOutput, writeTypeTag);

        }

        @Override
        public void reset() {
            // TODO Auto-generated method stub

        }

    }

    public static final class JUnorderedList extends JList {

        private AUnorderedListType listType;

        public JUnorderedList(IJObject jObject) {
            this.listType = new AUnorderedListType(jObject.getIAObject().getType(), null);
            this.jObjects = new ArrayList<IJObject>();
        }

        public JUnorderedList(IAType listItemType) {
            super();
            this.listType = new AUnorderedListType(listItemType, null);
            this.jObjects = new ArrayList<IJObject>();
        }

        @Override
        public void add(IJObject jObject) {
            jObjects.add(jObject);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.UNORDEREDLIST;
        }

        @Override
        public IAObject getIAObject() {
            AMutableUnorderedList v = new AMutableUnorderedList(listType);
            for (IJObject jObj : jObjects) {
                v.add(jObj.getIAObject());
            }
            return v;
        }

        public AUnorderedListType getListType() {
            return listType;
        }

        @Override
        public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
            IAsterixListBuilder listBuilder = new UnorderedListBuilder();
            listBuilder.reset(listType);
            ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
            for (IJObject jObject : jObjects) {
                fieldValue.reset();
                jObject.serialize(fieldValue.getDataOutput(), true);
                listBuilder.addItem(fieldValue);
            }
            listBuilder.write(dataOutput, writeTypeTag);
        }

        @Override
        public void reset() {
            jObjects.clear();
        }

    }

    public static final class JRecord implements IJObject {

        private AMutableRecord value;
        private ARecordType recordType;
        private IJObject[] fields;
        private Map<String, IJObject> openFields;
        private final AStringSerializerDeserializer aStringSerDer = AStringSerializerDeserializer.INSTANCE;

        public JRecord(ARecordType recordType, IJObject[] fields) {
            this.recordType = recordType;
            this.fields = fields;
            this.openFields = new LinkedHashMap<String, IJObject>();
        }

        public JRecord(ARecordType recordType, IJObject[] fields, LinkedHashMap<String, IJObject> openFields) {
            this.recordType = recordType;
            this.fields = fields;
            this.openFields = openFields;
        }

        public void addField(String fieldName, IJObject fieldValue) throws AsterixException {
            int pos = getFieldPosByName(fieldName);
            if (pos >= 0) {
                throw new AsterixException("field already defined in closed part");
            }
            if (openFields.get(fieldName) != null) {
                throw new AsterixException("field already defined in open part");
            }
            openFields.put(fieldName, fieldValue);
        }

        public IJObject getValueByName(String fieldName) throws AsterixException, IOException {
            // check closed part
            int fieldPos = getFieldPosByName(fieldName);
            if (fieldPos >= 0) {
                return fields[fieldPos];
            } else {
                // check open part
                IJObject fieldValue = openFields.get(fieldName);
                if (fieldValue == null) {
                    throw new AsterixException("unknown field: " + fieldName);
                }
                return fieldValue;
            }
        }

        public void setValueAtPos(int pos, IJObject jObject) {
            fields[pos] = jObject;
        }

        @Override
        public ATypeTag getTypeTag() {
            return recordType.getTypeTag();
        }

        public void setField(String fieldName, IJObject fieldValue) throws AsterixException {
            int pos = getFieldPosByName(fieldName);
            if (pos >= 0) {
                fields[pos] = fieldValue;
            } else {
                if (openFields.get(fieldName) != null) {
                    openFields.put(fieldName, fieldValue);
                } else {
                    throw new AsterixException("unknown field: " + fieldName);
                }
            }
        }

        private int getFieldPosByName(String fieldName) {
            int index = 0;
            String[] fieldNames = recordType.getFieldNames();
            for (String name : fieldNames) {
                if (name.equals(fieldName)) {
                    return index;
                }
                index++;
            }
            return -1;
        }

        public ARecordType getRecordType() {
            return recordType;
        }

        public IJObject[] getFields() {
            return fields;
        }

        public Map<String, IJObject> getOpenFields() {
            return this.openFields;
        }

        public RecordBuilder getRecordBuilder() {
            RecordBuilder recordBuilder = new RecordBuilder();
            recordBuilder.reset(recordType);
            return recordBuilder;
        }

        @Override
        public void serialize(DataOutput output, boolean writeTypeTag) throws HyracksDataException {
            RecordBuilder recordBuilder = new RecordBuilder();
            recordBuilder.reset(recordType);
            int index = 0;
            ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
            for (IJObject jObject : fields) {
                fieldValue.reset();
                jObject.serialize(fieldValue.getDataOutput(), writeTypeTag);
                recordBuilder.addField(index, fieldValue);
                index++;
            }

            try {
                if (openFields != null && !openFields.isEmpty()) {
                    ArrayBackedValueStorage openFieldName = new ArrayBackedValueStorage();
                    ArrayBackedValueStorage openFieldValue = new ArrayBackedValueStorage();
                    AMutableString nameValue = new AMutableString(""); // get from the pool
                    for (Entry<String, IJObject> entry : openFields.entrySet()) {
                        openFieldName.reset();
                        openFieldValue.reset();
                        nameValue.setValue(entry.getKey());
                        openFieldName.getDataOutput().write(ATypeTag.STRING.serialize());
                        aStringSerDer.serialize(nameValue, openFieldName.getDataOutput());
                        entry.getValue().serialize(openFieldValue.getDataOutput(), true);
                        recordBuilder.addField(openFieldName, openFieldValue);
                    }
                }
            } catch (IOException | AsterixException ae) {
                throw new HyracksDataException(ae);
            }
            try {
                recordBuilder.write(output, writeTypeTag);
            } catch (IOException | AsterixException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public IAObject getIAObject() {
            return value;
        }

        @Override
        public void reset() throws AlgebricksException {
            if (openFields != null && !openFields.isEmpty()) {
                openFields.clear();
            }
            if (fields != null) {
                for (IJObject field : fields) {
                    if (field != null) {
                        field.reset();
                    }
                }
            }
        }

        public void reset(IJObject[] fields, LinkedHashMap<String, IJObject> openFields) throws AlgebricksException {
            this.reset();
            this.fields = fields;
            this.openFields = openFields;
        }

    }

    public static class ByteArrayAccessibleInputStream extends ByteArrayInputStream {

        public ByteArrayAccessibleInputStream(byte[] buf, int offset, int length) {
            super(buf, offset, length);
        }

        public void setContent(byte[] buf, int offset, int length) {
            this.buf = buf;
            this.pos = offset;
            this.count = Math.min(offset + length, buf.length);
            this.mark = offset;
        }

        public byte[] getArray() {
            return buf;
        }

        public int getPosition() {
            return pos;
        }

        public int getCount() {
            return count;
        }

    }

    public static class ByteArrayAccessibleDataInputStream extends DataInputStream {

        public ByteArrayAccessibleDataInputStream(ByteArrayAccessibleInputStream in) {
            super(in);
        }

        public ByteArrayAccessibleInputStream getInputStream() {
            return (ByteArrayAccessibleInputStream) in;
        }

    }
}