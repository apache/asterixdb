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

import java.io.PrintStream;

import edu.uci.ics.asterix.om.base.AMutableUUID;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

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

        ps.print("uuid(\"" + uuid.toStringLiteralOnly() + "\")");
    }

}
