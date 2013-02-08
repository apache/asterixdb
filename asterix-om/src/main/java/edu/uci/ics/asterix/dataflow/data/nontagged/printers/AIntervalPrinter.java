/*
 * Copyright 2009-2011 by The Regents of the University of California
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

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class AIntervalPrinter implements IPrinter {

    private static final long serialVersionUID = 1L;

    public static final AIntervalPrinter INSTANCE = new AIntervalPrinter();

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.data.IPrinter#init()
     */
    @Override
    public void init() throws AlgebricksException {
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.algebricks.data.IPrinter#print(byte[], int, int, java.io.PrintStream)
     */
    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        ps.print("interval(\"");

        short typetag = AInt8SerializerDeserializer.getByte(b, s + 1 + 8 * 2);

        IPrinter timeInstancePrinter;

        if (typetag == ATypeTag.DATE.serialize()) {
            timeInstancePrinter = ADatePrinter.INSTANCE;
        } else if (typetag == ATypeTag.TIME.serialize()) {
            timeInstancePrinter = ATimePrinter.INSTANCE;
        } else if (typetag == ATypeTag.DATETIME.serialize()) {
            timeInstancePrinter = ADateTimePrinter.INSTANCE;
        } else {
            throw new AlgebricksException("Unsupport internal time types in interval: " + typetag);
        }

        if (typetag == ATypeTag.TIME.serialize() || typetag == ATypeTag.DATE.serialize()) {
            timeInstancePrinter.print(b, s + 1 + 4 - 1, 8, ps);
            ps.print(", ");
            timeInstancePrinter.print(b, s + 1 + 8 + 4 - 1, 8, ps);
        } else {
            timeInstancePrinter.print(b, s, 8, ps);
            ps.print(", ");
            timeInstancePrinter.print(b, s + 1 + 8 - 1, 8, ps);
        }

        ps.print("\")");
    }
}
