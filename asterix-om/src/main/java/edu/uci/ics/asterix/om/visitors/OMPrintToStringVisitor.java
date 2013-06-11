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
package edu.uci.ics.asterix.om.visitors;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.ABinary;
import edu.uci.ics.asterix.om.base.ABitArray;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ACircle;
import edu.uci.ics.asterix.om.base.ACollectionCursor;
import edu.uci.ics.asterix.om.base.ADate;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.ADayTimeDuration;
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
import edu.uci.ics.asterix.om.base.AYearMonthDuration;
import edu.uci.ics.asterix.om.base.IACursor;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;

public class OMPrintToStringVisitor implements IOMVisitor {

    private StringBuilder buffer;
    private ACollectionCursor collCur = new ACollectionCursor();

    public OMPrintToStringVisitor() {
        this.buffer = new StringBuilder();
    }

    public void reset() {
        buffer.setLength(0);
    }

    @Override
    public String toString() {
        return buffer.toString();
    }

    @Override
    public void visitABinary(ABinary obj) throws AsterixException {
        throw new NotImplementedException();
    }

    @Override
    public void visitABitArray(ABitArray obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitABoolean(ABoolean obj) throws AsterixException {
        buffer.append(obj.getBoolean());
    }

    @Override
    public void visitADate(ADate obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitADateTime(ADateTime obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitADouble(ADouble obj) throws AsterixException {
        buffer.append(obj.getDoubleValue());
    }

    @Override
    public void visitADuration(ADuration obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitAInterval(AInterval obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitAFloat(AFloat obj) throws AsterixException {
        buffer.append(obj.getFloatValue() + "f");
    }

    @Override
    public void visitAInt16(AInt16 obj) throws AsterixException {
        buffer.append(obj.getShortValue());
    }

    @Override
    public void visitAInt32(AInt32 obj) throws AsterixException {
        buffer.append(obj.getIntegerValue());
    }

    @Override
    public void visitAInt64(AInt64 obj) throws AsterixException {
        buffer.append(obj.getLongValue());
    }

    @Override
    public void visitAInt8(AInt8 obj) throws AsterixException {
        buffer.append(obj.getByteValue());
    }

    @Override
    public void visitANull(ANull obj) throws AsterixException {
        buffer.append("null");
    }

    @Override
    public void visitAOrderedList(AOrderedList obj) throws AsterixException {
        buffer.append("[ ");
        collCur.reset(obj);
        printListToBuffer(collCur);
        buffer.append("]");
    }

    @Override
    public void visitAPoint(APoint obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitAPoint3D(APoint3D obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitALine(ALine obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitAPolygon(APolygon obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitACircle(ACircle obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitARectangle(ARectangle obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitARecord(ARecord obj) throws AsterixException {
        buffer.append("{ ");
        int sz = obj.numberOfFields();
        ARecordType type = obj.getType();
        if (sz > 0) {
            for (int i = 0; i < sz - 1; i++) {
                buffer.append("\"");
                buffer.append(type.getFieldNames()[i]);
                buffer.append("\"");
                buffer.append(": ");
                obj.getValueByPos(i).accept(this);
                buffer.append(", ");
            }
            buffer.append("\"");
            buffer.append(type.getFieldNames()[sz - 1]);
            buffer.append("\"");
            buffer.append(": ");
            obj.getValueByPos(sz - 1).accept(this);
            buffer.append(" ");
        }
        buffer.append("}");
    }

    @Override
    public void visitAString(AString obj) throws AsterixException {
        buffer.append("\"");
        buffer.append(obj.getStringValue());
        buffer.append("\"");
    }

    @Override
    public void visitATime(ATime obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitAType(IAType obj) throws AsterixException {
        switch (obj.getTypeTag()) {
            case INT32: {
                buffer.append("int32");
                break;
            }
            case FLOAT: {
                buffer.append("float");
                break;
            }
            case STRING: {
                buffer.append("string");
                break;
            }
            default: {
                // TODO Auto-generated method stub
                throw new NotImplementedException("Pretty-printing is not implemented for type " + obj.getTypeTag()
                        + " .");
            }
        }

    }

    @Override
    public void visitAUnorderedList(AUnorderedList obj) throws AsterixException {
        buffer.append("{{");
        collCur.reset(obj);
        printListToBuffer(collCur);
        buffer.append("}}");
    }

    private void printListToBuffer(IACursor cursor) throws AsterixException {
        if (cursor.next()) {
            IAObject item0 = cursor.get();
            item0.accept(this);
            while (cursor.next()) {
                buffer.append(", ");
                IAObject item = cursor.get();
                item.accept(this);
            }
            buffer.append(" ");
        }
    }

    @Override
    public void visitAYearMonthDuration(AYearMonthDuration obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

    @Override
    public void visitADayTimeDuration(ADayTimeDuration obj) throws AsterixException {
        // TODO Auto-generated method stub
        throw new NotImplementedException();
    }

}
