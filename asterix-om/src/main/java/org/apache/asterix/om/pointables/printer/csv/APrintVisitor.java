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

package org.apache.asterix.om.pointables.printer.csv;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.printers.AUUIDPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.ShortWithoutTypeInfoPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ABooleanPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ACirclePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADatePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADateTimePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADayTimeDurationPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADoublePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADurationPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AFloatPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AInt16Printer;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AInt32Printer;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AInt64Printer;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AInt8Printer;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ALinePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ANullPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.APoint3DPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.APointPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.APolygonPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ARectanglePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AStringPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ATimePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AYearMonthDurationPrinter;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * This class is a IVisitablePointableVisitor implementation which recursively
 * visit a given record, list or flat value of a given type, and print it to a
 * PrintStream in CSV format.
 */
public class APrintVisitor implements IVisitablePointableVisitor<Void, Pair<PrintStream, ATypeTag>> {

    private final Map<IVisitablePointable, ARecordPrinter> raccessorToPrinter = new HashMap<IVisitablePointable, ARecordPrinter>();

    private int level = 0;

    @Override
    public Void visit(AListVisitablePointable accessor, Pair<PrintStream, ATypeTag> arg) throws AsterixException {
        throw new AsterixException("'List' type unsupported for CSV output");
    }

    @Override
    public Void visit(ARecordVisitablePointable accessor, Pair<PrintStream, ATypeTag> arg) throws AsterixException {
        ARecordPrinter printer = raccessorToPrinter.get(accessor);
        if (printer == null) {
            printer = new ARecordPrinter();
            raccessorToPrinter.put(accessor, printer);
        }
        if (level > 0) {
            throw new AsterixException("Nested 'Record' instances unsupported for CSV output");
        }

        try {
            level++;
            printer.printRecord(accessor, arg.first, this);
            level--;
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
                case YEARMONTHDURATION: {
                    AYearMonthDurationPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case DAYTIMEDURATION: {
                    ADayTimeDurationPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case UUID: {
                    AUUIDPrinter.INSTANCE.print(b, s, l, ps);
                    break;
                }
                case SHORTWITHOUTTYPEINFO: {
                    ShortWithoutTypeInfoPrinter.INSTANCE.print(b, s, l, ps);
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
