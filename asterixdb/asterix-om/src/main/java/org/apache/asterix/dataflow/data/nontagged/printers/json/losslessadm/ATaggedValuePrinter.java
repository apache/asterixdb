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

package org.apache.asterix.dataflow.data.nontagged.printers.json.losslessadm;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.bytes.HexPrinter;

public abstract class ATaggedValuePrinter implements IPrinter {

    public static final char DELIMITER = ':';

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws HyracksDataException {
        try {
            ps.print("\"");
            printTypeTag(b, s, ps);
            printDelimiter(ps);
            printNonTaggedValue(b, s, l, ps);
            ps.print("\"");
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    static void printTypeTag(byte[] b, int s, PrintStream ps) throws IOException {
        HexPrinter.printHexString(b, s, 1, ps);
    }

    static void printDelimiter(PrintStream ps) {
        ps.print(DELIMITER);
    }

    static void printFloat(byte[] b, int s, PrintStream ps) {
        int bits = AFloatSerializerDeserializer.getIntBits(b, s);
        ps.print(bits);
    }

    static void printDouble(byte[] b, int s, PrintStream ps) {
        long bits = ADoubleSerializerDeserializer.getLongBits(b, s);
        ps.print(bits);
    }

    protected abstract void printNonTaggedValue(byte[] b, int s, int l, PrintStream ps) throws IOException;
}
