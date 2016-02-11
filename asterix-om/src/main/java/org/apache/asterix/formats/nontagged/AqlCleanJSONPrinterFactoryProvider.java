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

import org.apache.asterix.dataflow.data.nontagged.printers.adm.AUUIDPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.adm.ShortWithoutTypeInfoPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ABinaryPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ABooleanPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ACirclePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ADatePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ADateTimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ADayTimeDurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ADoublePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ADurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AFloatPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AInt16PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AInt32PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AInt64PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AInt8PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AIntervalPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ALinePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ANullPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ANullableFieldPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AObjectPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AOrderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.APoint3DPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.APointPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.APolygonPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ARecordPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ARectanglePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AStringPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.ATimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AUnionPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AUnorderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AYearMonthDurationPrinterFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;

public class AqlCleanJSONPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final AqlCleanJSONPrinterFactoryProvider INSTANCE = new AqlCleanJSONPrinterFactoryProvider();

    private AqlCleanJSONPrinterFactoryProvider() {
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object type) {
        IAType aqlType = (IAType) type;

        if (aqlType != null) {
            switch (aqlType.getTypeTag()) {
                case INT8:
                    return AInt8PrinterFactory.INSTANCE;
                case INT16:
                    return AInt16PrinterFactory.INSTANCE;
                case INT32:
                    return AInt32PrinterFactory.INSTANCE;
                case INT64:
                    return AInt64PrinterFactory.INSTANCE;
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
                case INTERVAL:
                    return AIntervalPrinterFactory.INSTANCE;
                case POINT:
                    return APointPrinterFactory.INSTANCE;
                case POINT3D:
                    return APoint3DPrinterFactory.INSTANCE;
                case LINE:
                    return ALinePrinterFactory.INSTANCE;
                case POLYGON:
                    return APolygonPrinterFactory.INSTANCE;
                case CIRCLE:
                    return ACirclePrinterFactory.INSTANCE;
                case RECTANGLE:
                    return ARectanglePrinterFactory.INSTANCE;
                case STRING:
                    return AStringPrinterFactory.INSTANCE;
                case BINARY:
                    return ABinaryPrinterFactory.INSTANCE;
                case RECORD:
                    return new ARecordPrinterFactory((ARecordType) aqlType);
                case ORDEREDLIST:
                    return new AOrderedlistPrinterFactory((AOrderedListType) aqlType);
                case UNORDEREDLIST:
                    return new AUnorderedlistPrinterFactory((AUnorderedListType) aqlType);
                case UNION: {
                    if (((AUnionType) aqlType).isNullableType())
                        return new ANullableFieldPrinterFactory((AUnionType) aqlType);
                    else
                        return new AUnionPrinterFactory((AUnionType) aqlType);
                }
                case UUID: {
                    return AUUIDPrinterFactory.INSTANCE;
                }
                case SHORTWITHOUTTYPEINFO:
                    return ShortWithoutTypeInfoPrinterFactory.INSTANCE;
                case ANY:
                case BITARRAY:
                case ENUM:
                case SPARSERECORD:
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
