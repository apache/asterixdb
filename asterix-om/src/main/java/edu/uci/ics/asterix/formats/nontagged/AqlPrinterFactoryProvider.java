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

import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ABooleanPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ACirclePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADatePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADateTimePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADoublePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADurationPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AFloatPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AInt16PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AInt32PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AInt64PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AInt8PrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AIntervalPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ALinePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ANullPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ANullableFieldPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AObjectPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AOrderedlistPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.APoint3DPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.APointPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.APolygonPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ARecordPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ARectanglePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AStringPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ATimePrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AUnionPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AUnorderedlistPrinterFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactoryProvider;

public class AqlPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final AqlPrinterFactoryProvider INSTANCE = new AqlPrinterFactoryProvider();

    private AqlPrinterFactoryProvider() {
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
