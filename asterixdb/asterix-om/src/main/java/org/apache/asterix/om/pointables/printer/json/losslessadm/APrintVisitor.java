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

package org.apache.asterix.om.pointables.printer.json.losslessadm;

import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm.AObjectPrinterFactory;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.printer.AListPrinter;
import org.apache.asterix.om.pointables.printer.ARecordPrinter;
import org.apache.asterix.om.pointables.printer.AbstractPrintVisitor;
import org.apache.asterix.om.pointables.printer.IPrintVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class APrintVisitor extends AbstractPrintVisitor {

    private final org.apache.asterix.om.pointables.printer.json.clean.APrintVisitor cleanPrintVisitor =
            new org.apache.asterix.om.pointables.printer.json.clean.APrintVisitor();

    @Override
    protected AListPrinter createListPrinter(AListVisitablePointable accessor) {
        return new AListPrinter("[ ", " ]", ", ") {
            @Override
            protected ATypeTag getItemTypeTag(IVisitablePointable item, ATypeTag typeTag) {
                // avoid MISSING to NULL conversion, because we print MISSING as is in this format
                return typeTag;
            }
        };
    }

    @Override
    protected ARecordPrinter createRecordPrinter(ARecordVisitablePointable accessor) {
        return new ARecordPrinter("{ ", " }", ", ", ": ") {
            @Override
            protected void printFieldName(PrintStream ps, IPrintVisitor visitor, IVisitablePointable fieldName)
                    throws HyracksDataException {
                super.printFieldName(ps, cleanPrintVisitor, fieldName);
            }
        };
    }

    @Override
    protected boolean printFlatValue(ATypeTag typeTag, byte[] b, int s, int l, PrintStream ps)
            throws HyracksDataException {
        return AObjectPrinterFactory.printFlatValue(typeTag, b, s, l, ps);
    }
}
