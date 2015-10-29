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

package org.apache.asterix.dataflow.data.nontagged.printers.json.clean;

import org.apache.asterix.dataflow.data.nontagged.printers.PrintTools;
import org.apache.asterix.dataflow.data.nontagged.serde.ABinarySerializerDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.util.bytes.HexPrinter;

import java.io.IOException;
import java.io.PrintStream;

public class ABinaryHexPrinter implements IPrinter {
    private ABinaryHexPrinter() {
    }

    public static final ABinaryHexPrinter INSTANCE = new ABinaryHexPrinter();

    @Override public void init() throws AlgebricksException {

    }

    @Override public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {

        int validLength = ByteArrayPointable.getContentLength(b, s + 1);
        int start = s + 1 + ByteArrayPointable.getNumberBytesToStoreMeta(validLength);
        try {
            ps.print("\"");
            HexPrinter.printHexString(b, start, validLength, ps);
            ps.print("\"");
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

}
