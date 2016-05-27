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

package org.apache.asterix.dataflow.data.nontagged.printers.json.lossless;

import java.io.PrintStream;

import org.apache.asterix.om.base.AUUID;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;

public class AUUIDPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    public static final AUUIDPrinterFactory INSTANCE = new AUUIDPrinterFactory();

    public static final IPrinter PRINTER = (byte[] b, int s, int l, PrintStream ps) -> {
        StringBuilder buf = new StringBuilder(AUUID.UUID_CHARS + 2);
        buf.append('"');
        AUUID.appendLiteralOnly(b, s + 1, buf).append('"');
        ps.print(buf.toString());
    };

    @Override
    public IPrinter createPrinter() {
        return PRINTER;
    }
}
