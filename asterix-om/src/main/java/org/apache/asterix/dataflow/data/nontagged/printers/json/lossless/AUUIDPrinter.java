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

import org.apache.asterix.om.base.AMutableUUID;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.data.std.primitive.LongPointable;

import java.io.PrintStream;

public class AUUIDPrinter implements IPrinter {

    public static final AUUIDPrinter INSTANCE = new AUUIDPrinter();
    // We use mutable UUID not to create a UUID object multiple times.
    AMutableUUID uuid = new AMutableUUID(0, 0);

    @Override
    public void init() throws AlgebricksException {
    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long msb = LongPointable.getLong(b, s + 1);
        long lsb = LongPointable.getLong(b, s + 9);
        uuid.setValue(msb, lsb);

        ps.print("\"" + uuid.toStringLiteralOnly() + "\"");
    }

}
