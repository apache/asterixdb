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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.data.IPrinter;

public class AObjectPrinter implements IPrinter {

    public static final AObjectPrinter INSTANCE = new AObjectPrinter();

    private IPrinter recordPrinter = new ARecordPrinterFactory(null).createPrinter();
    private IPrinter orderedlistPrinter = new AOrderedlistPrinterFactory(null).createPrinter();
    private IPrinter unorderedListPrinter = new AUnorderedlistPrinterFactory(null).createPrinter();

    @Override
    public void init() throws AlgebricksException {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[s]);
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
            case INTERVAL: {
                AIntervalPrinter.INSTANCE.print(b, s, l, ps);
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
            case RECTANGLE: {
                ARectanglePrinter.INSTANCE.print(b, s, l, ps);
                break;
            }
            case CIRCLE: {
                ACirclePrinter.INSTANCE.print(b, s, l, ps);
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
            case RECORD: {
                this.recordPrinter.init();
                recordPrinter.print(b, s, l, ps);
                break;
            }
            case ORDEREDLIST: {
                this.orderedlistPrinter.init();
                orderedlistPrinter.print(b, s, l, ps);
                break;
            }
            case UNORDEREDLIST: {
                this.unorderedListPrinter.init();
                unorderedListPrinter.print(b, s, l, ps);
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
            case UNION:
            case UUID_STRING:
                // These are internal types and do not need a printer.
                throw new NotImplementedException("No printer for type " + typeTag);
        }
    }
}