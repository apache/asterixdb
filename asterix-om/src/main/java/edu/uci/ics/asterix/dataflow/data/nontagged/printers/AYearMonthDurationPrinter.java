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

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.utils.WriteValueTools;

public class AYearMonthDurationPrinter implements IPrinter {

    public static final AYearMonthDurationPrinter INSTANCE = new AYearMonthDurationPrinter();
    private static final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

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
        boolean positive = true;
        int months = AInt32SerializerDeserializer.getInt(b, s + 1);

        // set the negative flag. "||" is necessary in case that months field is not there (so it is 0)
        if (months < 0) {
            months *= -1;
            positive = false;
        }

        int month = gCalInstance.getDurationMonth(months);
        int year = gCalInstance.getDurationYear(months);

        ps.print("year-month-duration(\"");
        if (!positive) {
            ps.print("-");
        }
        try {
            ps.print("P");
            if (year != 0) {
                WriteValueTools.writeInt(year, ps);
                ps.print("Y");
            }
            if (month != 0) {
                WriteValueTools.writeInt(month, ps);
                ps.print("M");
            }
            ps.print("\")");
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

}
