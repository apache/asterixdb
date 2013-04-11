package edu.uci.ics.asterix.external.library.java;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AMutableCircle;
import edu.uci.ics.asterix.om.base.AMutableDate;
import edu.uci.ics.asterix.om.base.AMutableDateTime;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableDuration;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableInterval;
import edu.uci.ics.asterix.om.base.AMutableLine;
import edu.uci.ics.asterix.om.base.AMutableOrderedList;
import edu.uci.ics.asterix.om.base.AMutablePoint;
import edu.uci.ics.asterix.om.base.AMutablePoint3D;
import edu.uci.ics.asterix.om.base.AMutablePolygon;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableRectangle;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.AMutableUnorderedList;
import edu.uci.ics.asterix.om.base.APoint;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

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

    public static final class JInt implements IJObject {

        private AMutableInt32 value;

        public JInt(int value) {
            this.value = new AMutableInt32(value);
        }

        public void setValue(int v) {
            if (value == null) {
                value = new AMutableInt32(v);
            } else {
                ((AMutableInt32) value).setValue(v);
            }
        }

        public void setValue(AMutableInt32 v) {
            value = v;
        }

        public int getValue() {
            return ((AMutableInt32) value).getIntegerValue().intValue();
        }

        @Override
        public ATypeTag getTypeTag() {
            return BuiltinType.AINT32.getTypeTag();
        }

        @Override
        public IAObject getIAObject() {
            return value;
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

    }

    public static final class JString extends JObject {

        public JString(String v) {
            super(new AMutableString(v));
        }

        public void setValue(String v) {
            ((AMutableString) value).setValue(v);
        }

        public String getValue() {
            return ((AMutableString) value).getStringValue();
        }

    }

    public static final class JFloat implements IJObject {

        private AMutableFloat value;

        public JFloat(float v) {
            value = new AMutableFloat(v);
        }

        public void setValue(float v) {
            ((AMutableFloat) value).setValue(v);
        }

        public float getValue() {
            return ((AMutableFloat) value).getFloatValue();
        }

        @Override
        public ATypeTag getTypeTag() {
            return BuiltinType.AFLOAT.getTypeTag();
        }

        @Override
        public IAObject getIAObject() {
            return value;
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
    }

    public static final class JRectangle implements IJObject {

        private AMutableRectangle rect;

        public JRectangle(JPoint p1, JPoint p2) {
            rect = new AMutableRectangle((APoint) p1.getValue(), (APoint) p2.getValue());
        }

        public void setValue(JPoint p1, JPoint p2) {
            this.rect.setValue((APoint) p1.getValue(), (APoint) p2.getValue());
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.RECTANGLE;
        }

        @Override
        public IAObject getIAObject() {
            return rect;
        }

        @Override
        public String toString() {
            return rect.toString();
        }

    }

    public static final class JTime implements IJObject {

        private AMutableTime time;

        public JTime(int timeInMillsec) {
            time = new AMutableTime(timeInMillsec);
        }

        public void setValue(int timeInMillsec) {
            time.setValue(timeInMillsec);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.TIME;
        }

        @Override
        public IAObject getIAObject() {
            return time;
        }

        @Override
        public String toString() {
            return time.toString();
        }

    }

    public static final class JInterval implements IJObject {

        private AMutableInterval interval;

        public JInterval(long intervalStart, long intervalEnd) {
            interval = new AMutableInterval(intervalStart, intervalEnd, (byte) 0);
        }

        public void setValue(long intervalStart, long intervalEnd, byte typetag) {
            interval.setValue(intervalStart, intervalEnd, typetag);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.INTERVAL;
        }

        @Override
        public IAObject getIAObject() {
            return interval;
        }

        @Override
        public String toString() {
            return interval.toString();
        }

        public long getIntervalStart() {
            return interval.getIntervalStart();
        }

        public long getIntervalEnd() {
            return interval.getIntervalEnd();
        }

        public short getIntervalType() {
            return interval.getIntervalType();
        }

    }

    public static final class JDate implements IJObject {

        private AMutableDate date;

        public JDate(int chrononTimeInDays) {
            date = new AMutableDate(chrononTimeInDays);
        }

        public void setValue(int chrononTimeInDays) {
            date.setValue(chrononTimeInDays);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.DATE;
        }

        @Override
        public IAObject getIAObject() {
            return date;
        }

        @Override
        public String toString() {
            return date.toString();
        }

    }

    public static final class JDateTime implements IJObject {

        private AMutableDateTime dateTime;

        public JDateTime(long chrononTime) {
            dateTime = new AMutableDateTime(chrononTime);
        }

        public void setValue(long chrononTime) {
            dateTime.setValue(chrononTime);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.DATETIME;
        }

        @Override
        public IAObject getIAObject() {
            return dateTime;
        }

        @Override
        public String toString() {
            return dateTime.toString();
        }

    }

    public static final class JDuration implements IJObject {

        private AMutableDuration duration;

        public JDuration(int months, long milliseconds) {
            duration = new AMutableDuration(months, milliseconds);
        }

        public void setValue(int months, long milliseconds) {
            duration.setValue(months, milliseconds);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.DURATION;
        }

        @Override
        public IAObject getIAObject() {
            return duration;
        }

        @Override
        public String toString() {
            return duration.toString();
        }

    }

    public static final class JPolygon implements IJObject {

        private AMutablePolygon polygon;
        private List<JPoint> points;

        public JPolygon(List<JPoint> points) {
            this.points = points;
        }

        public void setValue(List<JPoint> points) {
            this.points = points;
            polygon = null;
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.POLYGON;
        }

        @Override
        public IAObject getIAObject() {
            if (polygon == null) {
                APoint[] pts = new APoint[points.size()];
                int index = 0;
                for (JPoint p : points) {
                    pts[index++] = (APoint) p.getIAObject();
                }
                polygon = new AMutablePolygon(pts);
            }
            return polygon;
        }

        @Override
        public String toString() {
            return getIAObject().toString();
        }

    }

    public static final class JCircle implements IJObject {

        private AMutableCircle circle;

        public JCircle(JPoint center, double radius) {
            circle = new AMutableCircle((APoint) center.getIAObject(), radius);
        }

        public void setValue(JPoint center, double radius) {
            circle.setValue((APoint) center.getIAObject(), radius);
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.CIRCLE;
        }

        @Override
        public IAObject getIAObject() {
            return circle;
        }

        @Override
        public String toString() {
            return circle.toString();
        }

    }

    public static final class JLine implements IJObject {

        private AMutableLine line;

        public JLine(JPoint p1, JPoint p2) {
            line = new AMutableLine((APoint) p1.getIAObject(), (APoint) p2.getIAObject());
        }

        public void setValue(JPoint p1, JPoint p2) {
            line.setValue((APoint) p1.getIAObject(), (APoint) p2.getIAObject());
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.LINE;
        }

        @Override
        public IAObject getIAObject() {
            return line;
        }

        @Override
        public String toString() {
            return line.toString();
        }

    }

    public static final class JPoint3D implements IJObject {

        private AMutablePoint3D value;

        public JPoint3D(double x, double y, double z) {
            value = new AMutablePoint3D(x, y, z);
        }

        public void setValue(double x, double y, double z) {
            value.setValue(x, y, z);
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

        public IAObject getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value.toString();
        }

        @Override
        public ATypeTag getTypeTag() {
            return ATypeTag.POINT3D;
        }

        @Override
        public IAObject getIAObject() {
            return value;
        }
    }

    public static final class JOrderedList implements IJObject {

        private AOrderedListType listType;
        private List<IJObject> jObjects;

        public JOrderedList(IJObject jObject) {
            this.listType = new AOrderedListType(jObject.getIAObject().getType(), null);
            this.jObjects = new ArrayList<IJObject>();
        }

        public void add(IJObject jObject) {
            jObjects.add(jObject);
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

    }

    public static final class JUnorderedList implements IJObject {

        private AUnorderedListType listType;
        private List<IJObject> jObjects;

        public JUnorderedList(IJObject jObject) {
            this.listType = new AUnorderedListType(jObject.getIAObject().getType(), null);
            this.jObjects = new ArrayList<IJObject>();
        }

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

        public boolean isEmpty() {
            return jObjects.isEmpty();
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

    }

    public static final class JRecord implements IJObject {

        private AMutableRecord value;
        private ARecordType recordType;
        private List<IJObject> fields;
        private List<String> fieldNames;
        private List<IAType> fieldTypes;
        private int numFieldsAdded = 0;
        private List<Boolean> openField;

        public JRecord(ARecordType recordType) {
            this.recordType = recordType;
            this.fields = new ArrayList<IJObject>();
            initFieldInfo();
        }

        public JRecord(ARecordType recordType, IJObject[] fields) {
            this.recordType = recordType;
            this.fields = new ArrayList<IJObject>();
            for (IJObject jObject : fields) {
                this.fields.add(jObject);
            }
            initFieldInfo();
        }

        public JRecord(String[] fieldNames, IJObject[] fields) throws AsterixException {
            this.recordType = getARecordType(fieldNames, fields);
            this.fields = new ArrayList<IJObject>();
            for (IJObject jObject : fields) {
                this.fields.add(jObject);
            }
            initFieldInfo();
        }

        private ARecordType getARecordType(String[] fieldNames, IJObject[] fields) throws AsterixException {
            IAType[] fieldTypes = new IAType[fields.length];
            int index = 0;
            for (IJObject jObj : fields) {
                fieldTypes[index++] = jObj.getIAObject().getType();
            }
            ARecordType recordType = new ARecordType(null, fieldNames, fieldTypes, false);
            return recordType;
        }

        private void initFieldInfo() {
            this.openField = new ArrayList<Boolean>();
            fieldNames = new ArrayList<String>();
            for (String name : recordType.getFieldNames()) {
                fieldNames.add(name);
                openField.add(false);
            }
            fieldTypes = new ArrayList<IAType>();
            for (IAType type : recordType.getFieldTypes()) {
                fieldTypes.add(type);
            }

        }

        private IAObject[] getIAObjectArray(List<IJObject> fields) {
            IAObject[] retValue = new IAObject[fields.size()];
            int index = 0;
            for (IJObject jObject : fields) {
                retValue[index++] = getIAObject(jObject);
            }
            return retValue;
        }

        private IAObject getIAObject(IJObject jObject) {
            IAObject retVal = null;
            switch (jObject.getTypeTag()) {
                case RECORD:
                    ARecordType recType = ((JRecord) jObject).getRecordType();
                    IAObject[] fields = new IAObject[((JRecord) jObject).getFields().size()];
                    int index = 0;
                    for (IJObject field : ((JRecord) jObject).getFields()) {
                        fields[index++] = getIAObject(field);
                    }
                    retVal = new AMutableRecord(recType, fields);
                default:
                    retVal = jObject.getIAObject();
                    break;
            }
            return retVal;
        }

        public void addField(String fieldName, IJObject fieldValue) {
            int pos = getFieldPosByName(fieldName);
            if (pos >= 0) {
                throw new IllegalArgumentException("field already defined");
            }
            numFieldsAdded++;
            fields.add(fieldValue);
            fieldNames.add(fieldName);
            fieldTypes.add(fieldValue.getIAObject().getType());
            openField.add(true);
        }

        public IJObject getValueByName(String fieldName) throws AsterixException, IOException {
            int fieldPos = getFieldPosByName(fieldName);
            if (fieldPos < 0) {
                throw new AsterixException("unknown field: " + fieldName);
            }
            return fields.get(fieldPos);
        }

        public void setValueAtPos(int pos, IJObject jtype) {
            fields.set(pos, jtype);
        }

        public void setValue(AMutableRecord mutableRecord) {
            this.value = mutableRecord;
            this.recordType = mutableRecord.getType();
        }

        @Override
        public ATypeTag getTypeTag() {
            return recordType.getTypeTag();
        }

        public void setField(String fieldName, IJObject fieldValue) {
            int pos = getFieldPosByName(fieldName);
            fields.set(pos, fieldValue);
        }

        private int getFieldPosByName(String fieldName) {
            int index = 0;
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

        public List<IJObject> getFields() {
            return fields;
        }

        @Override
        public IAObject getIAObject() {
            if (value == null || numFieldsAdded > 0) {
                value = new AMutableRecord(recordType, getIAObjectArray(fields));
            }
            return value;
        }

        public void close() {
            if (numFieldsAdded > 0) {
                int totalFields = fieldNames.size();
                for (int i = 0; i < numFieldsAdded; i++) {
                    fieldNames.remove(totalFields - 1 - i);
                    fieldTypes.remove(totalFields - 1 - i);
                    fields.remove(totalFields - 1 - i);
                }
                numFieldsAdded = 0;
            }
        }

        public List<Boolean> getOpenField() {
            return openField;
        }

        public List<String> getFieldNames() {
            return fieldNames;
        }

        public List<IAType> getFieldTypes() {
            return fieldTypes;
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