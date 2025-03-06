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

import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.DEFAULT_VALUES;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_DELIMITER;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_ESCAPE;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.KEY_QUOTE;
import static org.apache.asterix.dataflow.data.nontagged.printers.csv.CSVUtils.getCharOrDefault;

import java.io.PrintStream;

import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AStringPrinterFactory implements IPrinterFactory {
    private static final long serialVersionUID = 1L;
    private final boolean forceQuote;
    private final char quote;
    private final char escape;
    private final char delimiter;

    private AStringPrinterFactory(String quote, boolean forceQuote, String escape, String delimiter) {
        this.forceQuote = forceQuote;
        this.quote = getCharOrDefault(quote, DEFAULT_VALUES.get(KEY_QUOTE));
        this.escape = getCharOrDefault(escape, DEFAULT_VALUES.get(KEY_ESCAPE));
        this.delimiter = getCharOrDefault(delimiter, DEFAULT_VALUES.get(KEY_DELIMITER));
    }

    public static AStringPrinterFactory createInstance(String quote, String forceQuoteStr, String escape,
            String delimiter) {
        boolean forceQuote = forceQuoteStr == null || Boolean.parseBoolean(forceQuoteStr);
        return new AStringPrinterFactory(quote, forceQuote, escape, delimiter);
    }

    private void printString(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
        CSVUtils.printString(b, s, l, ps, quote, forceQuote, escape, delimiter);
    }

    @Override
    public IPrinter createPrinter(IEvaluatorContext context) {
        return this::printString;
    }
}
