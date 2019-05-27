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
package org.apache.asterix.app.result;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ResponsePrinter implements IResponsePrinter {

    private final SessionOutput sessionOutput;
    private final List<IResponseFieldPrinter> headers = new ArrayList<>();
    private final List<IResponseFieldPrinter> results = new ArrayList<>();
    private final List<IResponseFieldPrinter> footers = new ArrayList<>();
    private boolean headersPrinted = false;
    private boolean resultsPrinted = false;

    public ResponsePrinter(SessionOutput sessionOutput) {
        this.sessionOutput = sessionOutput;
    }

    @Override
    public void begin() {
        sessionOutput.hold();
        sessionOutput.out().print("{\n");
    }

    @Override
    public void addHeaderPrinter(IResponseFieldPrinter printer) {
        headers.add(printer);
    }

    @Override
    public void addResultPrinter(IResponseFieldPrinter printer) {
        results.add(printer);
    }

    @Override
    public void addFooterPrinter(IResponseFieldPrinter printer) {
        footers.add(printer);
    }

    @Override
    public void printHeaders() throws HyracksDataException {
        print(headers);
        headersPrinted = !headers.isEmpty();
    }

    @Override
    public void printResults() throws HyracksDataException {
        sessionOutput.release();
        print(results);
        if (!resultsPrinted) {
            resultsPrinted = !results.isEmpty();
        }
        results.clear();
    }

    @Override
    public void printFooters() throws HyracksDataException {
        print(footers);
    }

    @Override
    public void end() {
        sessionOutput.out().print("\n}\n");
        sessionOutput.release();
        sessionOutput.out().flush();
    }

    private void print(List<IResponseFieldPrinter> printers) throws HyracksDataException {
        final int fieldsCount = printers.size();
        if ((headersPrinted || resultsPrinted) && fieldsCount > 0) {
            printFieldSeparator(sessionOutput.out());
        }
        for (int i = 0; i < printers.size(); i++) {
            IResponseFieldPrinter printer = printers.get(i);
            printer.print(sessionOutput.out());
            if (i + 1 != fieldsCount) {
                printFieldSeparator(sessionOutput.out());
            }
        }
    }

    public static void printFieldSeparator(PrintWriter pw) {
        pw.print(",\n");
    }
}
