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

package org.apache.asterix.om.pointables.printer;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractPrintVisitor implements IPrintVisitor {
    private final Map<IVisitablePointable, ARecordPrinter> raccessorToPrinter = new HashMap<>();
    private final Map<IVisitablePointable, AListPrinter> laccessorToPrinter = new HashMap<>();

    @Override
    public Void visit(AListVisitablePointable accessor, Pair<PrintStream, ATypeTag> arg) throws HyracksDataException {
        AListPrinter printer = laccessorToPrinter.get(accessor);
        if (printer == null) {
            printer = createListPrinter(accessor);
            laccessorToPrinter.put(accessor, printer);
        }
        printer.printList(accessor, arg.first, this);
        return null;
    }

    @Override
    public Void visit(ARecordVisitablePointable accessor, Pair<PrintStream, ATypeTag> arg) throws HyracksDataException {
        ARecordPrinter printer = raccessorToPrinter.get(accessor);
        if (printer == null) {
            printer = createRecordPrinter(accessor);
            raccessorToPrinter.put(accessor, printer);
        }
        printer.printRecord(accessor, arg.first, this);
        return null;
    }

    @Override
    public Void visit(AFlatValuePointable accessor, Pair<PrintStream, ATypeTag> arg) throws HyracksDataException {
        byte[] b = accessor.getByteArray();
        int s = accessor.getStartOffset();
        int l = accessor.getLength();
        PrintStream ps = arg.first;
        ATypeTag typeTag = arg.second;
        if (!printFlatValue(typeTag, b, s, l, ps)) {
            throw new HyracksDataException("No printer for type " + typeTag);
        }
        return null;
    }

    protected abstract AListPrinter createListPrinter(AListVisitablePointable accessor) throws HyracksDataException;

    protected abstract ARecordPrinter createRecordPrinter(ARecordVisitablePointable accessor)
            throws HyracksDataException;

    protected abstract boolean printFlatValue(ATypeTag typeTag, byte[] b, int s, int l, PrintStream ps)
            throws HyracksDataException;
}
