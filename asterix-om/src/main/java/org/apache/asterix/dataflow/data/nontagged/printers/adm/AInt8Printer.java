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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.utils.WriteValueTools;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class AInt8Printer implements IPrinter {

    private static final String SUFFIX_STRING = "i8";

    public static final AInt8Printer INSTANCE = new AInt8Printer();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        byte o = AInt8SerializerDeserializer.getByte(b, s + 1);
        try {
            WriteValueTools.writeInt(o, ps);
            WriteValueTools.writeUTF8StringNoQuotes(SUFFIX_STRING, ps);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}