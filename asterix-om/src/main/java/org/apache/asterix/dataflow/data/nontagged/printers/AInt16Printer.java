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
package org.apache.asterix.dataflow.data.nontagged.printers;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.utils.WriteValueTools;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class AInt16Printer implements IPrinter {

    private static final String SUFFIX_STRING = "i16";
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

    public static final AInt16Printer INSTANCE = new AInt16Printer();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        short i = AInt16SerializerDeserializer.getShort(b, s + 1);
        try {
            WriteValueTools.writeInt(i, ps);
            WriteValueTools.writeUTF8StringNoQuotes(_suffix, 0, _suffix_count, ps);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}