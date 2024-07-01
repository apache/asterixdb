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
package org.apache.asterix.dataflow.data.nontagged.printers.csv;

import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_DELIMITER;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_EMPTY_FIELD_AS_NULL;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_ESCAPE;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_FORCE_QUOTE;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_NULL;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_QUOTE;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.printer.IPrintVisitor;
import org.apache.asterix.om.pointables.printer.csv.APrintVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AObjectPrinterFactory implements IPrinterFactory {
    private static final long serialVersionUID = 1L;
    private static final ConcurrentHashMap<String, AObjectPrinterFactory> instanceCache = new ConcurrentHashMap<>();
    private ARecordType itemType;
    private Map<String, String> configuration;
    private boolean emptyFieldAsNull;

    private AObjectPrinterFactory(ARecordType itemType, Map<String, String> configuration) {
        this.itemType = itemType;
        this.configuration = configuration;
        String emptyFieldAsNullStr = configuration.get(KEY_EMPTY_FIELD_AS_NULL);
        this.emptyFieldAsNull = emptyFieldAsNullStr != null && Boolean.parseBoolean(emptyFieldAsNullStr);
    }

    public static AObjectPrinterFactory createInstance(ARecordType itemType, Map<String, String> configuration) {
        // generate a unique identifier based on the parameters and hash the instance corresponding to it.
        String key = CSVUtils.generateKey(itemType, configuration);
        return instanceCache.computeIfAbsent(key, k -> new AObjectPrinterFactory(itemType, configuration));
    }

    public boolean printFlatValue(ATypeTag typeTag, byte[] b, int s, int l, PrintStream ps)
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
                ANullPrinterFactory.createInstance(configuration.get(KEY_NULL)).createPrinter().print(b, s, l, ps);
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
            case CIRCLE:
                ACirclePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case RECTANGLE:
                ARectanglePrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case STRING:
                if (emptyFieldAsNull && CSVUtils.isEmptyString(b, s, l)) {
                    ANullPrinterFactory.createInstance(configuration.get(KEY_NULL)).createPrinter().print(b, s, l, ps);
                } else {
                    AStringPrinterFactory
                            .createInstance(configuration.get(KEY_QUOTE), configuration.get(KEY_FORCE_QUOTE),
                                    configuration.get(KEY_ESCAPE), configuration.get(KEY_DELIMITER))
                            .createPrinter().print(b, s, l, ps);
                }
                return true;
            case BINARY:
                ABinaryHexPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            case UUID:
                AUUIDPrinterFactory.PRINTER.print(b, s, l, ps);
                return true;
            default:
                return false;
        }
    }

    @Override
    public IPrinter createPrinter() {
        final ARecordVisitablePointable recordVisitablePointable =
                new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        final Pair<PrintStream, ATypeTag> streamTag = new Pair<>(null, null);
        final IPrintVisitor visitor = new APrintVisitor(itemType, configuration);

        return (byte[] b, int s, int l, PrintStream ps) -> {
            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[s]);
            if (!printFlatValue(typeTag, b, s, l, ps)) {
                streamTag.first = ps;
                streamTag.second = typeTag;
                switch (typeTag) {
                    case OBJECT:
                        recordVisitablePointable.set(b, s, l);
                        visitor.visit(recordVisitablePointable, streamTag);
                        break;
                    default:
                        throw new HyracksDataException("No printer for type " + typeTag);
                }
            }
        };
    }
}
