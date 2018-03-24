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
package org.apache.asterix.dataflow.data.nontagged.printers.adm;

import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AGeometryPrinterFactory;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.printer.IPrintVisitor;
import org.apache.asterix.om.pointables.printer.adm.APrintVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AObjectPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final AObjectPrinterFactory INSTANCE = new AObjectPrinterFactory();

    public static boolean printFlatValue(ATypeTag typeTag, byte[] b, int s, int l, PrintStream ps)
            throws HyracksDataException {
        switch (typeTag) {
            case TINYINT:
                AInt8PrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case SMALLINT:
                AInt16PrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case INTEGER:
                AInt32PrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case BIGINT:
                AInt64PrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case MISSING:
            case NULL:
                ANullPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case BOOLEAN:
                ABooleanPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case FLOAT:
                AFloatPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case DOUBLE:
                ADoublePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case DATE:
                ADatePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case TIME:
                ATimePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case DATETIME:
                ADateTimePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case DURATION:
                ADurationPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case YEARMONTHDURATION:
                AYearMonthDurationPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case DAYTIMEDURATION:
                ADayTimeDurationPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case INTERVAL:
                AIntervalPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case POINT:
                APointPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case POINT3D:
                APoint3DPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case LINE:
                ALinePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case POLYGON:
                APolygonPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case RECTANGLE:
                ARectanglePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case CIRCLE:
                ACirclePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case STRING:
                AStringPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case BINARY:
                ABinaryHexPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case UUID:
                AUUIDPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case SHORTWITHOUTTYPEINFO:
                ShortWithoutTypeInfoPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case GEOMETRY:
                AGeometryPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            default:
                return false;
        }
    }

    @Override
    public IPrinter createPrinter() {
        final ARecordVisitablePointable rPointable =
                new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        final AListVisitablePointable olPointable =
                new AListVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        final AListVisitablePointable ulPointable =
                new AListVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE);
        final Pair<PrintStream, ATypeTag> streamTag = new Pair<>(null, null);

        final IPrintVisitor visitor = new APrintVisitor();

        return (byte[] b, int s, int l, PrintStream ps) -> {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[s]);
            if (!printFlatValue(typeTag, b, s, l, ps)) {
                streamTag.first = ps;
                streamTag.second = typeTag;
                switch (typeTag) {
                    case OBJECT:
                        rPointable.set(b, s, l);
                        visitor.visit(rPointable, streamTag);
                        break;
                    case ARRAY:
                        olPointable.set(b, s, l);
                        visitor.visit(olPointable, streamTag);
                        break;
                    case MULTISET:
                        ulPointable.set(b, s, l);
                        visitor.visit(ulPointable, streamTag);
                        break;
                    default:
                        throw new HyracksDataException("No printer for type " + typeTag);
                }
            }
        };
    }
}
