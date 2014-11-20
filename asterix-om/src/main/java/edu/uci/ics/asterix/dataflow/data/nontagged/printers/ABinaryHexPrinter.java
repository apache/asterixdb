/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ABinarySerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

import java.io.IOException;
import java.io.PrintStream;

public class ABinaryHexPrinter implements IPrinter {
    private ABinaryHexPrinter() {
    }

    public static final ABinaryHexPrinter INSTANCE = new ABinaryHexPrinter();

    @Override public void init() throws AlgebricksException {

    }

    @Override public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int validLength = ABinarySerializerDeserializer.getLength(b, s + 1);
        int start = s + 1 + ABinarySerializerDeserializer.SIZE_OF_LENGTH;
        try {
            ps.print("hex(\"");
            printHexString(b, start, validLength, ps);
            ps.print("\")");
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    public static Appendable printHexString(byte[] bytes, int start, int length, Appendable appendable)
            throws IOException {
        for (int i = 0; i < length; ++i) {
            appendable.append((char) PrintTools.hex((bytes[start + i] >>> 4) & 0x0f, PrintTools.CASE.UPPER_CASE));
            appendable.append((char) PrintTools.hex((bytes[start + i] & 0x0f), PrintTools.CASE.UPPER_CASE));
        }
        return appendable;
    }
}
