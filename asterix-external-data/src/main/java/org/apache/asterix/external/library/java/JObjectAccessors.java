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
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

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
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.external.api.IJListAccessor;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.api.IJObjectAccessor;
import org.apache.asterix.external.api.IJRecordAccessor;
import org.apache.asterix.external.library.TypeInfo;
import org.apache.asterix.external.library.java.JObjects.JBoolean;
import org.apache.asterix.external.library.java.JObjects.JByte;
import org.apache.asterix.external.library.java.JObjects.JCircle;
import org.apache.asterix.external.library.java.JObjects.JDate;
import org.apache.asterix.external.library.java.JObjects.JDateTime;
import org.apache.asterix.external.library.java.JObjects.JDouble;
import org.apache.asterix.external.library.java.JObjects.JDuration;
import org.apache.asterix.external.library.java.JObjects.JFloat;
import org.apache.asterix.external.library.java.JObjects.JInt;
import org.apache.asterix.external.library.java.JObjects.JInterval;
import org.apache.asterix.external.library.java.JObjects.JLine;
import org.apache.asterix.external.library.java.JObjects.JList;
import org.apache.asterix.external.library.java.JObjects.JLong;
import org.apache.asterix.external.library.java.JObjects.JOrderedList;
import org.apache.asterix.external.library.java.JObjects.JPoint;
import org.apache.asterix.external.library.java.JObjects.JPoint3D;
import org.apache.asterix.external.library.java.JObjects.JPolygon;
import org.apache.asterix.external.library.java.JObjects.JRecord;
import org.apache.asterix.external.library.java.JObjects.JRectangle;
import org.apache.asterix.external.library.java.JObjects.JString;
import org.apache.asterix.external.library.java.JObjects.JTime;
import org.apache.asterix.external.library.java.JObjects.JUnorderedList;
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.ALine;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.APoint3D;
import org.apache.asterix.om.base.APolygon;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.string.UTF8StringReader;

public class JObjectAccessors {

    public static IJObjectAccessor createFlatJObjectAccessor(ATypeTag aTypeTag) {
        IJObjectAccessor accessor = null;
        switch (aTypeTag) {
            case BOOLEAN:
                accessor = new JBooleanAccessor();
                break;
            case INT8:
                accessor = new JInt8Accessor();
                break;
            case INT16:
                accessor = new JInt16Accessor();
                break;
            case INT32:
                accessor = new JInt32Accessor();
                break;
            case INT64:
                accessor = new JInt64Accessor();
                break;
            case FLOAT:
                accessor = new JFloatAccessor();
                break;
            case DOUBLE:
                accessor = new JDoubleAccessor();
                break;
            case STRING:
                accessor = new JStringAccessor();
                break;
            case POINT:
                accessor = new JPointAccessor();
                break;
            case POINT3D:
                accessor = new JPoint3DAccessor();
                break;
            case LINE:
                accessor = new JLineAccessor();
                break;
            case DATE:
                accessor = new JDateAccessor();
                break;
            case DATETIME:
                accessor = new JDateTimeAccessor();
                break;
            case DURATION:
                accessor = new JDurationAccessor();
                break;
            default:
                break;
        }
        return accessor;
    }

    public static class JInt8Accessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            byte o = AInt8SerializerDeserializer.getByte(b, s + 1);
            IJObject jObject = objectPool.allocate(BuiltinType.AINT8);
            ((JByte) jObject).setValue(o);
            return null;
        }

    }

    public static class JInt16Accessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            short i = AInt16SerializerDeserializer.getShort(b, s + 1);
            IJObject jObject = objectPool.allocate(BuiltinType.AINT16);
            ((JInt) jObject).setValue(i);
            return null;
        }
    }

    public static class JInt32Accessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int i = AInt32SerializerDeserializer.getInt(b, s + 1);
            IJObject jObject = objectPool.allocate(BuiltinType.AINT32);
            ((JInt) jObject).setValue(i);
            return jObject;
        }
    }

    public static class JInt64Accessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            long v = AInt64SerializerDeserializer.getLong(b, s + 1);
            IJObject jObject = objectPool.allocate(BuiltinType.AINT64);
            ((JLong) jObject).setValue(v);
            return jObject;
        }
    }

    public static class JFloatAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            float v = AFloatSerializerDeserializer.getFloat(b, s + 1);
            IJObject jObject = objectPool.allocate(BuiltinType.AFLOAT);
            ((JFloat) jObject).setValue(v);
            return jObject;
        }
    }

    public static class JDoubleAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            double v = ADoubleSerializerDeserializer.getDouble(b, s + 1);
            IJObject jObject = objectPool.allocate(BuiltinType.ADOUBLE);
            ((JDouble) jObject).setValue(v);
            return jObject;
        }
    }

    public static class JStringAccessor implements IJObjectAccessor {
        private final UTF8StringReader reader = new UTF8StringReader();

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();

            String v = null;
            try {
                v = reader.readUTF(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            JObjectUtil.getNormalizedString(v);

            IJObject jObject = objectPool.allocate(BuiltinType.ASTRING);
            ((JString) jObject).setValue(JObjectUtil.getNormalizedString(v));
            return jObject;
        }
    }

    public static class JBooleanAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            Boolean v = ABooleanSerializerDeserializer.getBoolean(b, s);
            IJObject jObject = objectPool.allocate(BuiltinType.ABOOLEAN);
            ((JBoolean) jObject).setValue(v);
            return jObject;
        }
    }

    public static class JDateAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int v = ADateSerializerDeserializer.getChronon(b, s);
            IJObject jObject = objectPool.allocate(BuiltinType.ADATE);
            ((JDate) jObject).setValue(v);
            return jObject;
        }
    }

    public static class JDateTimeAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            long v = ADateTimeSerializerDeserializer.getChronon(b, s);
            IJObject jObject = objectPool.allocate(BuiltinType.ADATETIME);
            ((JDateTime) jObject).setValue(v);
            return jObject;
        }
    }

    public static class JDurationAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();
            ADuration duration = ADurationSerializerDeserializer.INSTANCE
                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s, l)));
            IJObject jObject = objectPool.allocate(BuiltinType.ADURATION);
            ((JDuration) jObject).setValue(duration.getMonths(), duration.getMilliseconds());
            return jObject;
        }
    }

    public static class JTimeAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int v = ATimeSerializerDeserializer.getChronon(b, s);
            IJObject jObject = objectPool.allocate(BuiltinType.ATIME);
            ((JTime) jObject).setValue(v);
            return jObject;
        }
    }

    public static class JIntervalAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            long intervalStart = AIntervalSerializerDeserializer.getIntervalStart(b, s);
            long intervalEnd = AIntervalSerializerDeserializer.getIntervalEnd(b, s);
            byte intervalType = AIntervalSerializerDeserializer.getIntervalTimeType(b, s);
            IJObject jObject = objectPool.allocate(BuiltinType.AINTERVAL);
            try {
                ((JInterval) jObject).setValue(intervalStart, intervalEnd, intervalType);
            } catch (AlgebricksException e) {
                throw new HyracksDataException(e);
            }
            return jObject;
        }
    }

    // Spatial Types

    public static class JCircleAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();
            ACircle v = ACircleSerializerDeserializer.INSTANCE
                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s, l)));
            JPoint jpoint = (JPoint) objectPool.allocate(BuiltinType.APOINT);
            jpoint.setValue(v.getP().getX(), v.getP().getY());
            IJObject jObject = objectPool.allocate(BuiltinType.ACIRCLE);
            ((JCircle) jObject).setValue(jpoint, v.getRadius());
            return jObject;
        }
    }

    public static class JPointAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();
            APoint v = APointSerializerDeserializer.INSTANCE
                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s, l)));
            JPoint jObject = (JPoint) objectPool.allocate(BuiltinType.APOINT);
            jObject.setValue(v.getX(), v.getY());
            return jObject;
        }
    }

    public static class JPoint3DAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();
            APoint3D v = APoint3DSerializerDeserializer.INSTANCE
                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s, l)));
            JPoint3D jObject = (JPoint3D) objectPool.allocate(BuiltinType.APOINT3D);
            jObject.setValue(v.getX(), v.getY(), v.getZ());
            return jObject;
        }
    }

    public static class JLineAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();
            ALine v = ALineSerializerDeserializer.INSTANCE
                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s, l)));
            JLine jObject = (JLine) objectPool.allocate(BuiltinType.ALINE);
            jObject.setValue(v.getP1(), v.getP2());
            return jObject;
        }
    }

    public static class JPolygonAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();
            APolygon v = APolygonSerializerDeserializer.INSTANCE
                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s, l)));
            JPolygon jObject = (JPolygon) objectPool.allocate(BuiltinType.APOLYGON);
            jObject.setValue(v.getPoints());
            return jObject;
        }
    }

    public static class JRectangleAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();
            ARectangle v = ARectangleSerializerDeserializer.INSTANCE
                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s, l)));
            JRectangle jObject = (JRectangle) objectPool.allocate(BuiltinType.ARECTANGLE);
            jObject.setValue(v.getP1(), v.getP2());
            return jObject;
        }
    }

    public static class JRecordAccessor implements IJRecordAccessor {

        private final TypeInfo typeInfo;
        private final JRecord jRecord;
        private final IJObject[] jObjects;
        private final LinkedHashMap<String, IJObject> openFields;
        private final UTF8StringReader reader = new UTF8StringReader();

        public JRecordAccessor(ARecordType recordType, IObjectPool<IJObject, IAType> objectPool) {
            this.typeInfo = new TypeInfo(objectPool, null, null);
            this.jObjects = new IJObject[recordType.getFieldNames().length];
            this.jRecord = new JRecord(recordType, jObjects);
            this.openFields = new LinkedHashMap<String, IJObject>();
        }

        @Override
        public JRecord access(ARecordVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool,
                ARecordType recordType, JObjectPointableVisitor pointableVisitor) throws HyracksDataException {
            try {
                jRecord.reset();
            } catch (AlgebricksException e) {
                throw new HyracksDataException(e);
            }
            ARecordVisitablePointable recordPointable = pointable;
            List<IVisitablePointable> fieldPointables = recordPointable.getFieldValues();
            List<IVisitablePointable> fieldTypeTags = recordPointable.getFieldTypeTags();
            List<IVisitablePointable> fieldNames = recordPointable.getFieldNames();
            int index = 0;
            boolean closedPart = true;
            try {
                IJObject fieldObject = null;
                for (IVisitablePointable fieldPointable : fieldPointables) {
                    closedPart = index < recordType.getFieldTypes().length;
                    IVisitablePointable tt = fieldTypeTags.get(index);
                    IAType fieldType = closedPart ? recordType.getFieldTypes()[index] : null;
                    ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                            .deserialize(tt.getByteArray()[tt.getStartOffset()]);
                    IVisitablePointable fieldName = fieldNames.get(index);
                    typeInfo.reset(fieldType, typeTag);
                    switch (typeTag) {
                        case RECORD:
                            fieldObject = pointableVisitor.visit((ARecordVisitablePointable) fieldPointable, typeInfo);
                            break;
                        case ORDEREDLIST:
                        case UNORDEREDLIST:
                            if (fieldPointable instanceof AFlatValuePointable) {
                                // value is null
                                fieldObject = null;
                            } else {
                                fieldObject = pointableVisitor.visit((AListVisitablePointable) fieldPointable,
                                        typeInfo);
                            }
                            break;
                        case ANY:
                            break;
                        default:
                            fieldObject = pointableVisitor.visit((AFlatValuePointable) fieldPointable, typeInfo);
                    }
                    if (closedPart) {
                        jObjects[index] = fieldObject;
                    } else {
                        byte[] b = fieldName.getByteArray();
                        int s = fieldName.getStartOffset();
                        int l = fieldName.getLength();
                        String v = reader.readUTF(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
                        openFields.put(v, fieldObject);
                    }
                    index++;
                    fieldObject = null;
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw new HyracksDataException(e);
            }
            return jRecord;
        }

        public void reset() throws HyracksDataException {
            try {
                jRecord.reset();
            } catch (AlgebricksException e) {
                throw new HyracksDataException(e);
            }
            openFields.clear();
        }

    }

    public static class JListAccessor implements IJListAccessor {

        private final TypeInfo typeInfo;

        public JListAccessor(IObjectPool<IJObject, IAType> objectPool) {
            this.typeInfo = new TypeInfo(objectPool, null, null);
        }

        @Override
        public IJObject access(AListVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool,
                IAType listType, JObjectPointableVisitor pointableVisitor) throws HyracksDataException {
            List<IVisitablePointable> items = pointable.getItems();
            List<IVisitablePointable> itemTags = pointable.getItemTags();
            JList list = pointable.ordered() ? new JOrderedList(listType) : new JUnorderedList(listType);
            IJObject listItem = null;
            int index = 0;
            try {

                for (IVisitablePointable itemPointable : items) {
                    IVisitablePointable itemTagPointable = itemTags.get(index);
                    ATypeTag itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                            .deserialize(itemTagPointable.getByteArray()[itemTagPointable.getStartOffset()]);
                    typeInfo.reset(listType.getType(), listType.getTypeTag());
                    switch (itemTypeTag) {
                        case RECORD:
                            listItem = pointableVisitor.visit((ARecordVisitablePointable) itemPointable, typeInfo);
                            break;
                        case UNORDEREDLIST:
                        case ORDEREDLIST:
                            listItem = pointableVisitor.visit((AListVisitablePointable) itemPointable, typeInfo);
                            break;
                        case ANY:
                            throw new IllegalArgumentException(
                                    "Cannot parse list item of type " + listType.getTypeTag());
                        default:
                            IAType itemType = ((AbstractCollectionType) listType).getItemType();
                            typeInfo.reset(itemType, itemType.getTypeTag());
                            listItem = pointableVisitor.visit((AFlatValuePointable) itemPointable, typeInfo);

                    }
                    list.add(listItem);
                }
            } catch (AsterixException exception) {
                throw new HyracksDataException(exception);
            }
            return list;
        }
    }

    public static class JUnorderedListAccessor implements IJObjectAccessor {

        @Override
        public IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool)
                throws HyracksDataException {
            return null;
        }

    }
}
