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

package org.apache.asterix.om.pointables.printer.json.lossless;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.dataflow.data.nontagged.printers.adm.ShortWithoutTypeInfoPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ABinaryHexPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ABooleanPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ACirclePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADatePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADateTimePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADayTimeDurationPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADoublePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ADurationPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AFloatPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AInt16Printer;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AInt32Printer;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AInt64Printer;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AInt8Printer;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ALinePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ANullPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.APoint3DPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.APointPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.APolygonPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ARectanglePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AStringPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.ATimePrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AUUIDPrinter;
import org.apache.asterix.dataflow.data.nontagged.printers.json.lossless.AYearMonthDurationPrinter;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.common.utils.Pair;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is a IVisitablePointableVisitor implementation which recursively
 * visit a given record, list or flat value of a given type, and print it to a
 * PrintStream in JSON format.
 */
public class APrintVisitor implements IVisitablePointableVisitor<Void, Pair<PrintStream, ATypeTag>> {

    private final Map<IVisitablePointable, ARecordPrinter> raccessorToPrinter = new HashMap<IVisitablePointable, ARecordPrinter>();
    private final Map<IVisitablePointable, AListPrinter> laccessorToPrinter = new HashMap<IVisitablePointable, AListPrinter>();

    @Override
    public Void visit(AListVisitablePointable accessor, Pair<PrintStream, ATypeTag> arg) throws AsterixException {
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
    public Void visit(ARecordVisitablePointable accessor, Pair<PrintStream, ATypeTag> arg) throws AsterixException {
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
                case BINARY: {
                    ABinaryHexPrinter.INSTANCE.print(b, s, l, ps);
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
