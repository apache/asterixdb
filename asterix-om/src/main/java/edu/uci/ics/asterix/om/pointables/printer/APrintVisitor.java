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

package edu.uci.ics.asterix.om.pointables.printer;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ABooleanPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ACirclePrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADatePrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADateTimePrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADayTimeDurationPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADoublePrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ADurationPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AFloatPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AInt16Printer;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AInt32Printer;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AInt64Printer;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AInt8Printer;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AIntervalPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ALinePrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ANullPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.APoint3DPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.APointPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.APolygonPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ARectanglePrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AStringPrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ATimePrinter;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.AYearMonthDurationPrinter;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

/**
 * This class is a IVisitablePointableVisitor implementation which recursively
 * visit a given record, list or flat value of a given type, and print it to a
 * PrintStream in adm format.
 */
public class APrintVisitor implements IVisitablePointableVisitor<Void, Pair<PrintStream, ATypeTag>> {

    private final Map<IVisitablePointable, ARecordPrinter> raccessorToPrinter = new HashMap<IVisitablePointable, ARecordPrinter>();
    private final Map<IVisitablePointable, AListPrinter> laccessorToPrinter = new HashMap<IVisitablePointable, AListPrinter>();

    @Override
    public Void visit(AListPointable accessor, Pair<PrintStream, ATypeTag> arg) throws AsterixException {
        AListPrinter printer = laccessorToPrinter.get(accessor);
        if (printer == null) {
            printer = new AListPrinter(accessor.ordered());
            laccessorToPrinter.put(accessor, printer);
        }
        try {
            printer.printList(accessor, arg.first, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return null;
    }

    @Override
    public Void visit(ARecordPointable accessor, Pair<PrintStream, ATypeTag> arg) throws AsterixException {
        ARecordPrinter printer = raccessorToPrinter.get(accessor);
        if (printer == null) {
            printer = new ARecordPrinter();
            raccessorToPrinter.put(accessor, printer);
        }
        try {
            printer.printRecord(accessor, arg.first, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return null;
    }

    @Override
    public Void visit(AFlatValuePointable accessor, Pair<PrintStream, ATypeTag> arg) {
        try {
            byte[] b = accessor.getByteArray();
            int s = accessor.getStartOffset();
            int l = accessor.getLength();
            PrintStream ps = arg.first;
            ATypeTag typeTag = arg.second;
            switch (typeTag) {
                case INT8: {
                    AInt8Printer.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case INT16: {
                    AInt16Printer.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case INT32: {
                    AInt32Printer.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case INT64: {
                    AInt64Printer.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case NULL: {
                    ANullPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case BOOLEAN: {
                    ABooleanPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case FLOAT: {
                    AFloatPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case DOUBLE: {
                    ADoublePrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case DATE: {
                    ADatePrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case TIME: {
                    ATimePrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case DATETIME: {
                    ADateTimePrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case DURATION: {
                    ADurationPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case YEARMONTHDURATION: {
                    AYearMonthDurationPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case DAYTIMEDURATION: {
                    ADayTimeDurationPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case POINT: {
                    APointPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case POINT3D: {
                    APoint3DPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case LINE: {
                    ALinePrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case POLYGON: {
                    APolygonPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case CIRCLE: {
                    ACirclePrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case RECTANGLE: {
                    ARectanglePrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case STRING: {
                    AStringPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case INTERVAL: {
                    AIntervalPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                default: {
                    throw new NotImplementedException("No printer for type " + typeTag);
                }
            }
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
