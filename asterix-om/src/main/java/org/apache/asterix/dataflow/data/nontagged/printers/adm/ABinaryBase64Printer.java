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

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.util.bytes.Base64Printer;

public class ABinaryBase64Printer implements IPrinter {
    private ABinaryBase64Printer() {
    }

    public static final ABinaryBase64Printer INSTANCE = new ABinaryBase64Printer();

    @Override
    public void init() throws AlgebricksException {
    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int validLength = ByteArrayPointable.getContentLength(b, s + 1);
        int start = s + 1 + ByteArrayPointable.getNumberBytesToStoreMeta(validLength);
        try {
            ps.print("base64(\"");
            Base64Printer.printBase64Binary(b, start, validLength, ps);
            ps.print("\")");
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

}
