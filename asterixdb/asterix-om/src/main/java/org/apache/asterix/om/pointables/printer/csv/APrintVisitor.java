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
import java.util.Map;

import org.apache.asterix.dataflow.data.nontagged.printers.csv.AObjectPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.printer.AListPrinter;
import org.apache.asterix.om.pointables.printer.ARecordPrinter;
import org.apache.asterix.om.pointables.printer.AbstractPrintVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This class is an IVisitablePointableVisitor implementation which recursively
 * visits a given record, list or flat value of a given type, and prints it to a
 * PrintStream in CSV format.
 */
public class APrintVisitor extends AbstractPrintVisitor {
    private final IEvaluatorContext context;
    private final ARecordType itemType;
    private final Map<String, String> formatConfigs;
    private final Map<String, String> configuration;
    private AObjectPrinterFactory objectPrinterFactory;

    public APrintVisitor(IEvaluatorContext context, ARecordType itemType, Map<String, String> formatConfigs,
            Map<String, String> configuration) {
        super();
        this.context = context;
        this.itemType = itemType;
        this.formatConfigs = formatConfigs;
        this.configuration = configuration;
    }

    @Override
    protected AListPrinter createListPrinter(AListVisitablePointable accessor) throws HyracksDataException {
        throw new HyracksDataException("'List' type unsupported for CSV output");
    }

    @Override
    protected ARecordPrinter createRecordPrinter(ARecordVisitablePointable accessor) {
        boolean header = CSVUtils.getHeader(configuration);
        String fieldSeparator = CSVUtils.getDelimiter(configuration);
        String recordDelimiter = CSVUtils.getRecordDelimiter(configuration, itemType != null);
        return new ACSVRecordPrinter(context.getWarningCollector(), header, fieldSeparator, recordDelimiter, itemType);
    }

    @Override
    protected boolean printFlatValue(ATypeTag typeTag, byte[] b, int s, int l, PrintStream ps)
            throws HyracksDataException {
        if (objectPrinterFactory == null) {
            objectPrinterFactory = AObjectPrinterFactory.createInstance(itemType, formatConfigs, configuration);
        }
        return objectPrinterFactory.printFlatValue(typeTag, b, s, l, ps);
    }
}
