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
package org.apache.asterix.om.visitors;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.ABitArray;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADayTimeDuration;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.ALine;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.APoint3D;
import org.apache.asterix.om.base.APolygon;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.AYearMonthDuration;
import org.apache.asterix.om.base.ShortWithoutTypeInfo;
import org.apache.asterix.om.types.IAType;

public interface IOMVisitor {
    public void visitABoolean(ABoolean obj) throws AsterixException;

    public void visitADouble(ADouble obj) throws AsterixException;

    public void visitAFloat(AFloat obj) throws AsterixException;

    public void visitAInt8(AInt8 obj) throws AsterixException;

    public void visitAInt16(AInt16 obj) throws AsterixException;

    public void visitAInt32(AInt32 obj) throws AsterixException;

    public void visitAInt64(AInt64 obj) throws AsterixException;

    public void visitAString(AString obj) throws AsterixException;

    public void visitADuration(ADuration obj) throws AsterixException;

    public void visitAYearMonthDuration(AYearMonthDuration obj) throws AsterixException;

    public void visitADayTimeDuration(ADayTimeDuration obj) throws AsterixException;

    public void visitAInterval(AInterval obj) throws AsterixException;

    public void visitADate(ADate obj) throws AsterixException;

    public void visitATime(ATime obj) throws AsterixException;

    public void visitADateTime(ADateTime obj) throws AsterixException;

    public void visitABitArray(ABitArray obj) throws AsterixException;

    public void visitABinary(ABinary obj) throws AsterixException;

    public void visitAOrderedList(AOrderedList obj) throws AsterixException;

    public void visitAUnorderedList(AUnorderedList obj) throws AsterixException;

    public void visitARecord(ARecord obj) throws AsterixException;

    public void visitANull(ANull obj) throws AsterixException;

    public void visitAPoint(APoint obj) throws AsterixException;

    public void visitAPoint3D(APoint3D obj) throws AsterixException;

    public void visitAType(IAType obj) throws AsterixException;

    public void visitALine(ALine obj) throws AsterixException;

    public void visitAPolygon(APolygon obj) throws AsterixException;

    public void visitACircle(ACircle obj) throws AsterixException;

    public void visitARectangle(ARectangle obj) throws AsterixException;

    public void visitAUUID(AUUID obj) throws AsterixException;

    public void visitShortWithoutTypeInfo(ShortWithoutTypeInfo obj) throws AsterixException;

}
