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

import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ABinaryPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ABooleanPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ACirclePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ADatePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ADateTimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ADayTimeDurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ADoublePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ADurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AFloatPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AInt16PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AInt32PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AInt64PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AInt8PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ALinePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AMissingPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ANullPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AObjectPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AOptionalFieldPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AOrderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.APoint3DPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.APointPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.APolygonPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ARecordPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ARectanglePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AStringPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.ATimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AUUIDPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AUnionPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AUnorderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AYearMonthDurationPrinterFactory;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADayTimeDuration;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.ALine;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.APoint3D;
import org.apache.asterix.om.base.APolygon;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.base.AYearMonthDuration;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Printer for the Lossless-ADM-JSON format.
 * <p/>
 * Format specification:
 * <p/>
 * <ul>
 * <li>Record is printed as JSON object
 * <li>Array or multiset is printed as JSON array
 * <li>Primitive types are printed as JSON strings in the following format: {@code "TC:encoded_value"}. <br/>
 *     TC is a two-character type code returned by {@link ATypeTag#serialize()} printed in hexadecimal format, <br/>
 *     ':' is a separator character, <br/>
 *     {@code encoded_value} is a type-specific text. <br/>
 *     This is a generic primitive type encoding that works for all primitive types.
 *     Null, Boolean, Int64, and String types types are printed in their special simplified form describe below.
 *     Lossless-ADM-JSON parser is supposed to read both forms for these types: generic and simplified.
 * </ul>
 *
 * <p/>
 * encoded_value format in the generic form:
 * <ul>
 * <li>Int8, Int16, Int32: integer number
 * <li>Float, Double: integer number corresponding to IEEE 754 floating-point layout</li>
 * <li>Date: integer number returned by {@link ADate#getChrononTimeInDays()}</li>
 * <li>Time: integer number returned by {@link ATime#getChrononTime()}</li>
 * <li>DateTime: integer number returned by {@link ADateTime#getChrononTime()} </li>
 * <li>YearMonthDuration: integer number returned by {@link AYearMonthDuration#getMonths()}</li>
 * <li>DayTimeDuration: integer number returned by {@link ADayTimeDuration#getMilliseconds()}</li>
 * <li>Duration: int1:int2, where int1 is {@link ADuration#getMonths()} and int2 is {@link ADuration#getMilliseconds()}</li>
 * <li>Binary: {@link ABinary#getBytes()} encoded as Base64</li>
 * <li>UUID: textual form produced by {@link AUUID#appendLiteralOnly(Appendable)}</li>
 * <li>Missing: N/A. The two character type code (TC) is sufficient to encode this type. ':' separator character is not printed</li>
 * <li>Point: int1:int2, where int1 and int2 are integer numbers corresponding to IEEE 754 floating-point layout of
 *     {@link APoint#getX()} and {@link APoint#getY()}</li>
 * <li>Point3D: int1:int2:int3, where int1 and int2, and int3 are integer numbers corresponding to IEEE 754 floating-point layouts of
 *     {@link APoint3D#getX()} and {@link APoint3D#getY()}, and {@link APoint3D#getZ()}</li>
 * <li>Circle: int1:int2:int3 - integer numbers corresponding to IEEE 754 floating-point layouts of {@link ACircle#getP()} (getX()/getY()) and {@link ACircle#getRadius()}</li>
 * <li>Line: int1:int2:int3:int4 - integer numbers corresponding to IEEE 754 floating-point layouts of {@link ALine#getP1()} (getX(), getY()) and {@link ALine#getP2()} (getX(), getY())</li>
 * <li>Rectangle:  int1:int2:int3:int4 - integer numbers corresponding to IEEE 754 floating-point layouts of {@link ARectangle#getP1()} (getX(), getY()) and {@link ARectangle#getP2()} (getX(), getY())</li></li>
 * <li>Polygon: int0:int1:int2:..intN - integer numbers where int0 is the number of points and each subsequent integer pair corresponds to IEEE 754 floating-point layouts of each point returned by {@link APolygon#getPoints()}</li>
 * </ul>
 *
 * Simplified form for {@code Null}, {@code Boolean}, {@code Int64}, and {@code String} types:
 * <ul>
 * <li>Null is printed as JSON null
 * <li>Boolean value is printed as JSON boolean
 * <li>Int64 value is printed as JSON number
 * <li>String value is printed as ":string_text". Empty string is printed as "".
 * </ul>
 *
 * Generic encoded_value form for {@code Null}, {@code Boolean}, {@code Int64}, and {@code String} types
 * <ul>
 * <li>Null: N/A. The two character type code (TC) is sufficient to encode this type. ':' separator character is not printed</li>
 * <li>Boolean: integer number. 0=false, non-0=true
 * <li>Int64: integer number
 * <li>String: string text as is
 * </ul>
 */
public class LosslessADMJSONPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final LosslessADMJSONPrinterFactoryProvider INSTANCE = new LosslessADMJSONPrinterFactoryProvider();

    private LosslessADMJSONPrinterFactoryProvider() {
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object typeInfo) throws HyracksDataException {
        IAType type = (IAType) typeInfo;

        if (type != null) {
            switch (type.getTypeTag()) {
                case TINYINT:
                    return AInt8PrinterFactory.INSTANCE;
                case SMALLINT:
                    return AInt16PrinterFactory.INSTANCE;
                case INTEGER:
                    return AInt32PrinterFactory.INSTANCE;
                case BIGINT:
                    return AInt64PrinterFactory.INSTANCE;
                case MISSING:
                    return AMissingPrinterFactory.INSTANCE;
                case NULL:
                    return ANullPrinterFactory.INSTANCE;
                case BOOLEAN:
                    return ABooleanPrinterFactory.INSTANCE;
                case FLOAT:
                    return AFloatPrinterFactory.INSTANCE;
                case DOUBLE:
                    return ADoublePrinterFactory.INSTANCE;
                case TIME:
                    return ATimePrinterFactory.INSTANCE;
                case DATE:
                    return ADatePrinterFactory.INSTANCE;
                case DATETIME:
                    return ADateTimePrinterFactory.INSTANCE;
                case DURATION:
                    return ADurationPrinterFactory.INSTANCE;
                case YEARMONTHDURATION:
                    return AYearMonthDurationPrinterFactory.INSTANCE;
                case DAYTIMEDURATION:
                    return ADayTimeDurationPrinterFactory.INSTANCE;
                case STRING:
                    return AStringPrinterFactory.INSTANCE;
                case BINARY:
                    return ABinaryPrinterFactory.INSTANCE;
                case UUID:
                    return AUUIDPrinterFactory.INSTANCE;
                case POINT:
                    return APointPrinterFactory.INSTANCE;
                case POINT3D:
                    return APoint3DPrinterFactory.INSTANCE;
                case CIRCLE:
                    return ACirclePrinterFactory.INSTANCE;
                case LINE:
                    return ALinePrinterFactory.INSTANCE;
                case RECTANGLE:
                    return ARectanglePrinterFactory.INSTANCE;
                case POLYGON:
                    return APolygonPrinterFactory.INSTANCE;
                case OBJECT:
                    return new ARecordPrinterFactory((ARecordType) type);
                case ARRAY:
                    return new AOrderedlistPrinterFactory((AOrderedListType) type);
                case MULTISET:
                    return new AUnorderedlistPrinterFactory((AUnorderedListType) type);
                case UNION:
                    if (((AUnionType) type).isUnknownableType()) {
                        return new AOptionalFieldPrinterFactory((AUnionType) type);
                    } else {
                        return new AUnionPrinterFactory((AUnionType) type);
                    }
                case GEOMETRY:
                case INTERVAL:
                    // NYI
                case SHORTWITHOUTTYPEINFO:
                case ANY:
                case BITARRAY:
                case ENUM:
                case SPARSOBJECT:
                case SYSTEM_NULL:
                case TYPE:
                case UINT16:
                case UINT32:
                case UINT64:
                case UINT8:
                    // These types are not intended to be printed to the user.
                    break;
            }
        }
        return AObjectPrinterFactory.INSTANCE;
    }
}
