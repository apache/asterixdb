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
import edu.uci.ics.asterix.om.base.ARectangle;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.IAType;

/**
 * The mechanism by which results are iterated over. Results from ASTERIX may
 * come in the form of a set of objects which may either be primitives (e.g.
 * int, string, ...), collections (e.g. ordered lists, unordered lists, ...),
 * records, or some combination thereof.
 * 
 * @author zheilbron
 */
public interface IADMCursor {
    public ABinary getBinary() throws AQLJException;

    public ABinary getBinary(String field) throws AQLJException;

    public ABitArray getBitArray() throws AQLJException;

    public ABitArray getBitArray(String field) throws AQLJException;

    public ABoolean getBoolean() throws AQLJException;

    public ABoolean getBoolean(String field) throws AQLJException;

    public ACircle getCircle() throws AQLJException;

    public ACircle getCircle(String field) throws AQLJException;

    public ADate getDate() throws AQLJException;

    public ADate getDate(String field) throws AQLJException;

    public ADateTime getDateTime() throws AQLJException;

    public ADateTime getDateTime(String field) throws AQLJException;

    public ADouble getDouble() throws AQLJException;

    public ADouble getDouble(String field) throws AQLJException;

    public ADuration getDuration() throws AQLJException;

    public ADuration getDuration(String field) throws AQLJException;

    public AFloat getFloat() throws AQLJException;

    public AFloat getFloat(String field) throws AQLJException;

    public AInt8 getInt8() throws AQLJException;

    public AInt8 getInt8(String field) throws AQLJException;

    public AInt16 getInt16() throws AQLJException;

    public AInt16 getInt16(String field) throws AQLJException;

    public AInt32 getInt32() throws AQLJException;

    public AInt32 getInt32(String field) throws AQLJException;

    public AInt64 getInt64() throws AQLJException;

    public AInt64 getInt64(String field) throws AQLJException;

    public ALine getLine() throws AQLJException;

    public ALine getLine(String field) throws AQLJException;

    public APoint getPoint() throws AQLJException;

    public APoint getPoint(String field) throws AQLJException;

    public APoint3D getPoint3D() throws AQLJException;

    public APoint3D getPoint3D(String field) throws AQLJException;

    public APolygon getPolygon() throws AQLJException;

    public APolygon getPolygon(String field) throws AQLJException;

    public ARectangle getRectangle() throws AQLJException;

    public ARectangle getRectangle(String field) throws AQLJException;

    public AString getString(String field) throws AQLJException;

    public AString getString() throws AQLJException;

    public ATime getTime() throws AQLJException;

    public ATime getTime(String field) throws AQLJException;

    /**
     * Advances the cursor to the next object
     * 
     * @return true if the cursor points to a an object
     * @throws AQLJException
     */
    public boolean next() throws AQLJException;

    /**
     * Positions the cursor c on the object pointed to by this
     * 
     * @param c
     *            the cursor to position
     * @throws AQLJException
     */
    public void position(IADMCursor c) throws AQLJException;

    /**
     * Positions the cursor c on the object associated with the given field
     * 
     * @param c
     *            the cursor to position
     * @param field
     *            the field name
     * @throws AQLJException
     */
    public void position(IADMCursor c, String field) throws AQLJException;

    /**
     * Returns the type of the current object being pointed at, which may be
     * null.
     * 
     * @return the type of the current object
     */
    public IAType getType();

    /**
     * Returns the current object being pointed at, which may be null.
     * 
     * @return the current object
     */
    public IAObject get();
}
