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

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class APolygonPrinter implements IPrinter {

    public static final APolygonPrinter INSTANCE = new APolygonPrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        short numberOfPoints = AInt16SerializerDeserializer.getShort(b, s + 1);
        s += 3;
        ps.print("polygon(\"");
        for (int i = 0; i < numberOfPoints; i++) {
            if (i > 0)
                ps.print(" ");
            ps.print(ADoubleSerializerDeserializer.getDouble(b, s));
            ps.print(",");
            ps.print(ADoubleSerializerDeserializer.getDouble(b, s + 8));
            s += 16;
        }
        ps.print("\")");

    }
}