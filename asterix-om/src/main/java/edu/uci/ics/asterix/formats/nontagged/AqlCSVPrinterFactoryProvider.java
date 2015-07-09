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
package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AUUIDPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ShortWithoutTypeInfoPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ABooleanPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ACirclePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ADatePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ADateTimePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ADayTimeDurationPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ADoublePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ADurationPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AFloatPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AInt16PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AInt32PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AInt64PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AInt8PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AIntervalPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ALinePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ANullPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ANullableFieldPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AObjectPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.APoint3DPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.APointPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.APolygonPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ARecordPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ARectanglePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AStringPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.ATimePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AUnionPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.csv.AYearMonthDurationPrinterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;

public class AqlCSVPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final AqlCSVPrinterFactoryProvider INSTANCE = new AqlCSVPrinterFactoryProvider();

    private AqlCSVPrinterFactoryProvider() {
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object type) throws AlgebricksException {
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
                case RECORD:
                    return new ARecordPrinterFactory((ARecordType) aqlType);
                case ORDEREDLIST:
                    throw new AlgebricksException("'Orderedlist' type unsupported for CSV output");
                case UNORDEREDLIST:
                    throw new AlgebricksException("'Unorderedlist' type unsupported for CSV output");
                case UNION: {
                    if (NonTaggedFormatUtil.isOptionalField((AUnionType) aqlType))
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
                case BINARY:
                case BITARRAY:
                case ENUM:
                case SPARSERECORD:
                case SYSTEM_NULL:
                case TYPE:
                case UINT16:
                case UINT32:
                case UINT64:
                case UINT8:
                case UUID_STRING:
                    // These types are not intended to be printed to the user.
                    break;
            }
        }
        return AObjectPrinterFactory.INSTANCE;

    }
}
