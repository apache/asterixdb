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

import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ABooleanPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ACirclePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ADatePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ADateTimePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ADayTimeDurationPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ADoublePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ADurationPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AFloatPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AInt16PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AInt32PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AInt64PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AInt8PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ALinePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ANullPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ANullableFieldPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AObjectPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AOrderedlistPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.APoint3DPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.APointPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.APolygonPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ARecordPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ARectanglePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AStringPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.ATimePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AUnionPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AUnorderedlistPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AYearMonthDurationPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.json.AYearMonthDurationPrinterFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;

public class AqlJSONPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final AqlJSONPrinterFactoryProvider INSTANCE = new AqlJSONPrinterFactoryProvider();

    private AqlJSONPrinterFactoryProvider() {
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object type) throws AlgebricksException {
        IAType aqlType = (IAType) type;

        if (aqlType != null) {
            switch (aqlType.getTypeTag()) {
            // case ANYTYPE:
            // return AAnyTypePrinterFactory.INSTANCE;
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
                    return new AOrderedlistPrinterFactory((AOrderedListType) aqlType);
                case UNORDEREDLIST:
                    return new AUnorderedlistPrinterFactory((AUnorderedListType) aqlType);
                case UNION: {
                    if (NonTaggedFormatUtil.isOptionalField((AUnionType) aqlType))
                        return new ANullableFieldPrinterFactory((AUnionType) aqlType);
                    else
                        return new AUnionPrinterFactory((AUnionType) aqlType);
                }
            }
        }
        return AObjectPrinterFactory.INSTANCE;

    }
}
