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

import java.io.PrintStream;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;

public class ANullPrinterFactory implements IPrinterFactory {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_NULL_STRING = "";
    // Store the information about the instance based on the parameters
    private static final ConcurrentHashMap<String, ANullPrinterFactory> instanceCache = new ConcurrentHashMap<>();
    private String nullString;

    private ANullPrinterFactory(String nullString) {
        this.nullString = nullString;
    }

    public static ANullPrinterFactory createInstance(String nullString) {
        String key = CSVUtils.generateKey(nullString);
        return instanceCache.computeIfAbsent(key, k -> new ANullPrinterFactory(nullString));
    }

    private final IPrinter PRINTER = (byte[] b, int s, int l, PrintStream ps) -> {
        if (nullString != null) {
            ps.print(nullString);
        } else {
            ps.print(DEFAULT_NULL_STRING);
        }
    };

    @Override
    public IPrinter createPrinter() {
        return PRINTER;
    }
}
