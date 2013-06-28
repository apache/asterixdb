/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.utils.WriteValueTools;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class AInt64Printer implements IPrinter {

    private static final String SUFFIX_STRING = "i64";
    private static byte[] _suffix;
    private static int _suffix_count;
    static {
        ByteArrayAccessibleOutputStream interm = new ByteArrayAccessibleOutputStream();
        DataOutput dout = new DataOutputStream(interm);
        try {
            dout.writeUTF(SUFFIX_STRING);
            interm.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        _suffix = interm.getByteArray();
        _suffix_count = interm.size();
    }

    public static final AInt64Printer INSTANCE = new AInt64Printer();

    @Override
    public void init() {
    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long d = AInt64SerializerDeserializer.getLong(b, s + 1);
        try {
            WriteValueTools.writeLong(d, ps);
            WriteValueTools.writeUTF8StringNoQuotes(_suffix, 0, _suffix_count, ps);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}