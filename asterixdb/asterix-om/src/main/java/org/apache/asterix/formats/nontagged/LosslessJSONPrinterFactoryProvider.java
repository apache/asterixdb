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

import org.apache.asterix.dataflow.data.nontagged.printers.adm.ShortWithoutTypeInfoPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ABinaryHexPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ABooleanPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ACirclePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADatePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADateTimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADayTimeDurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADoublePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AFloatPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AInt16PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AInt32PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AInt64PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AInt8PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AIntervalPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ALinePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ANullPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AObjectPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AOptionalFieldPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AOrderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.APoint3DPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.APointPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.APolygonPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ARecordPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ARectanglePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AStringPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ATimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AUUIDPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AUnionPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AUnorderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AYearMonthDurationPrinterFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;

public class LosslessJSONPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final LosslessJSONPrinterFactoryProvider INSTANCE = new LosslessJSONPrinterFactoryProvider();

    private LosslessJSONPrinterFactoryProvider() {
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object typeInfo) {
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
                    return ABinaryHexPrinterFactory.INSTANCE;
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
                case UUID:
                    return AUUIDPrinterFactory.INSTANCE;
                case SHORTWITHOUTTYPEINFO:
                    return ShortWithoutTypeInfoPrinterFactory.INSTANCE;
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
