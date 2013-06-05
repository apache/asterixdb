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
package edu.uci.ics.asterix.dataflow.data.nontagged.printers.json;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class AFloatPrinter implements IPrinter {

    public static final AFloatPrinter INSTANCE = new AFloatPrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        ps.print(AFloatSerializerDeserializer.getFloat(b, s + 1));
    }
}