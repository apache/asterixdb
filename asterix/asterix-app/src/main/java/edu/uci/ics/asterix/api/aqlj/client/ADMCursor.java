/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.api.aqlj.client;

import edu.uci.ics.asterix.api.aqlj.common.AQLJException;
import edu.uci.ics.asterix.om.base.ABinary;
import edu.uci.ics.asterix.om.base.ABitArray;
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
import edu.uci.ics.asterix.om.base.ALine;
import edu.uci.ics.asterix.om.base.APoint;
import edu.uci.ics.asterix.om.base.APoint3D;
import edu.uci.ics.asterix.om.base.APolygon;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.ARectangle;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.IACollection;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.IAType;

/**
 * This class is the implementation of IADMCursor. This class supports iterating
 * over all objects in ASTERIX. All ASTERIX objects can be iterated over and
 * returned via the associated get<Type>() call.
 * 
 * @author zheilbron
 */
public class ADMCursor implements IADMCursor {
    protected IAObject currentObject;
    protected IACursor collectionCursor;
    private boolean readOnce;

    public ADMCursor(IAObject currentObject) {
        setCurrentObject(currentObject);
    }

    public boolean next() throws AQLJException {
        if (collectionCursor != null) {
            boolean next = collectionCursor.next();
            if (next) {
                currentObject = collectionCursor.get();
            }
            return next;
        } else if (currentObject == null) {
            return false;
        } else {
            if (!readOnce) {
                readOnce = true;
                return true;
            }
        }
        return false;
    }

    @Override
    public void position(IADMCursor c) throws AQLJException {
        ((ADMCursor) c).setCurrentObject(currentObject);
    }

    @Override
    public void position(IADMCursor c, String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        ((ADMCursor) c).setCurrentObject(o);
    }

    private IAObject getObjectByField(String field) throws AQLJException {
        ATypeTag tag = currentObject.getType().getTypeTag();
        if (tag != ATypeTag.RECORD) {
            throw new AQLJException("object of type " + tag + " has no fields");
        }
        ARecord curRecord = (ARecord) currentObject;
        ARecordType t = curRecord.getType();
        int idx = t.findFieldPosition(field);
        if (idx == -1) {
            return null;
        }
        IAObject o = curRecord.getValueByPos(idx);
        return o;
    }

    public void setCurrentObject(IAObject o) {
        readOnce = false;
        currentObject = o;
        if (currentObject != null) {
            if (currentObject.getType() instanceof AbstractCollectionType) {
                collectionCursor = ((IACollection) currentObject).getCursor();
            }
        }
    }

    private void checkTypeTag(IAObject o, ATypeTag expectedTag) throws AQLJException {
        ATypeTag actualTag;
        actualTag = o.getType().getTypeTag();

        if (actualTag != expectedTag) {
            throw new AQLJException("cannot get " + expectedTag + " when type is " + actualTag);
        }
    }

    @Override
    public ABinary getBinary() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.BINARY);
        return ((ABinary) currentObject);
    }

    @Override
    public ABinary getBinary(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.BINARY);
        return (ABinary) o;
    }

    @Override
    public ABitArray getBitArray() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.BITARRAY);
        return ((ABitArray) currentObject);
    }

    @Override
    public ABitArray getBitArray(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.BITARRAY);
        return (ABitArray) o;
    }

    @Override
    public ABoolean getBoolean() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.BOOLEAN);
        return ((ABoolean) currentObject);
    }

    @Override
    public ABoolean getBoolean(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.BOOLEAN);
        return (ABoolean) o;
    }

    @Override
    public ACircle getCircle() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.CIRCLE);
        return ((ACircle) currentObject);
    }

    @Override
    public ACircle getCircle(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.CIRCLE);
        return (ACircle) o;
    }

    @Override
    public ADate getDate() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.DATE);
        return ((ADate) currentObject);
    }

    @Override
    public ADate getDate(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.DATE);
        return (ADate) o;
    }

    @Override
    public ADateTime getDateTime() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.DATETIME);
        return ((ADateTime) currentObject);
    }

    @Override
    public ADateTime getDateTime(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.DATETIME);
        return (ADateTime) o;
    }

    @Override
    public ADouble getDouble() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.DOUBLE);
        return ((ADouble) currentObject);
    }

    @Override
    public ADouble getDouble(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.DOUBLE);
        return (ADouble) o;
    }

    @Override
    public ADuration getDuration() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.DURATION);
        return ((ADuration) currentObject);
    }

    @Override
    public ADuration getDuration(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.DURATION);
        return (ADuration) o;
    }

    @Override
    public AFloat getFloat() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.FLOAT);
        return ((AFloat) currentObject);
    }

    @Override
    public AFloat getFloat(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.FLOAT);
        return (AFloat) o;
    }

    @Override
    public AInt8 getInt8() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.INT8);
        return ((AInt8) currentObject);
    }

    @Override
    public AInt8 getInt8(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.INT8);
        return (AInt8) o;
    }

    @Override
    public AInt16 getInt16() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.INT16);
        return ((AInt16) currentObject);
    }

    @Override
    public AInt16 getInt16(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.INT16);
        return (AInt16) o;
    }

    @Override
    public AInt32 getInt32() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.INT32);
        return ((AInt32) currentObject);
    }

    @Override
    public AInt32 getInt32(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.INT32);
        return (AInt32) o;
    }

    @Override
    public AInt64 getInt64() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.INT64);
        return ((AInt64) currentObject);
    }

    @Override
    public AInt64 getInt64(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.INT64);
        return (AInt64) o;
    }

    @Override
    public ALine getLine() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.LINE);
        return ((ALine) currentObject);
    }

    @Override
    public ALine getLine(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.LINE);
        return (ALine) o;
    }

    @Override
    public APoint getPoint() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.POINT);
        return ((APoint) currentObject);
    }

    @Override
    public APoint getPoint(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.POINT);
        return (APoint) o;
    }

    @Override
    public APoint3D getPoint3D() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.POINT3D);
        return ((APoint3D) currentObject);
    }

    @Override
    public APoint3D getPoint3D(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.POINT3D);
        return (APoint3D) o;
    }

    @Override
    public APolygon getPolygon() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.POLYGON);
        return ((APolygon) currentObject);
    }

    @Override
    public APolygon getPolygon(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.POLYGON);
        return (APolygon) o;
    }

    @Override
    public ARectangle getRectangle() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.RECTANGLE);
        return ((ARectangle) currentObject);
    }

    @Override
    public ARectangle getRectangle(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.RECTANGLE);
        return (ARectangle) o;
    }

    @Override
    public AString getString() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.STRING);
        return ((AString) currentObject);
    }

    @Override
    public AString getString(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.STRING);
        return (AString) o;
    }

    @Override
    public ATime getTime() throws AQLJException {
        checkTypeTag(currentObject, ATypeTag.TIME);
        return ((ATime) currentObject);
    }

    @Override
    public ATime getTime(String field) throws AQLJException {
        IAObject o = getObjectByField(field);
        checkTypeTag(o, ATypeTag.TIME);
        return (ATime) o;
    }

    public IAType getType() {
        if (currentObject != null) {
            return currentObject.getType();
        }
        return null;
    }

    @Override
    public IAObject get() {
        return currentObject;
    }
}
