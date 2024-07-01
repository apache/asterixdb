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

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.dataflow.data.nontagged.printers.PrintTools;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AStringPrinterFactory implements IPrinterFactory {
    private static final long serialVersionUID = 1L;
    private static final ConcurrentHashMap<String, AStringPrinterFactory> instanceCache = new ConcurrentHashMap<>();
    private static final String NONE = "none";
    private String quote;
    private Boolean forceQuote;
    private String escape;
    private String delimiter;

    private AStringPrinterFactory(String quote, Boolean forceQuote, String escape, String delimiter) {
        this.quote = quote;
        this.forceQuote = forceQuote;
        this.escape = escape;
        this.delimiter = delimiter;
    }

    public static AStringPrinterFactory createInstance(String quote, String forceQuoteStr, String escape,
            String delimiter) {
        boolean forceQuote = forceQuoteStr == null || Boolean.parseBoolean(forceQuoteStr);
        String key = CSVUtils.generateKey(quote, forceQuoteStr, escape, delimiter);
        return instanceCache.computeIfAbsent(key, k -> new AStringPrinterFactory(quote, forceQuote, escape, delimiter));
    }

    private final IPrinter PRINTER = (byte[] b, int s, int l, PrintStream ps) -> {
        try {
            char quoteChar =
                    quote == null ? extractSingleChar(DEFAULT_VALUES.get(KEY_QUOTE)) : extractSingleChar(quote);
            char escapeChar =
                    escape == null ? extractSingleChar(DEFAULT_VALUES.get(KEY_ESCAPE)) : extractSingleChar(escape);
            char delimiterChar = delimiter == null ? extractSingleChar(DEFAULT_VALUES.get(KEY_DELIMITER))
                    : extractSingleChar(delimiter);
            PrintTools.writeUTF8StringAsCSV(b, s + 1, l - 1, ps, quoteChar, forceQuote, escapeChar, delimiterChar);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    };

    public char extractSingleChar(String input) throws IOException {
        if (input != null && input.length() == 1) {
            return input.charAt(0);
        } else if (input.equalsIgnoreCase(NONE)) {
            return CSVUtils.NULL_CHAR; // Replace 'none' with null character
        } else {
            throw new IOException("Input string must be a single character");
        }
    }

    @Override
    public IPrinter createPrinter() {
        return PRINTER;
    }
}
