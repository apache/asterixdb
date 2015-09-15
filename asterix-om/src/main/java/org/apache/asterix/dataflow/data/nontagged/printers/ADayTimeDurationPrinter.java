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

import java.io.IOException;
import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.utils.WriteValueTools;

public class ADayTimeDurationPrinter implements IPrinter {

    public static final ADayTimeDurationPrinter INSTANCE = new ADayTimeDurationPrinter();
    private static final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

    @Override
    public void init() throws AlgebricksException {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        boolean positive = true;
        long milliseconds = AInt64SerializerDeserializer.getLong(b, s + 1);

        // set the negative flag. "||" is necessary in case that months field is not there (so it is 0)
        if (milliseconds < 0) {
            milliseconds *= -1;
            positive = false;
        }

        int millisecond = gCalInstance.getDurationMillisecond(milliseconds);
        int second = gCalInstance.getDurationSecond(milliseconds);
        int minute = gCalInstance.getDurationMinute(milliseconds);
        int hour = gCalInstance.getDurationHour(milliseconds);
        int day = gCalInstance.getDurationDay(milliseconds);

        ps.print("day-time-duration(\"");
        if (!positive) {
            ps.print("-");
        }
        try {
            ps.print("P");
            if (day != 0) {
                WriteValueTools.writeInt(day, ps);
                ps.print("D");
            }
            if (hour != 0 || minute != 0 || second != 0 || millisecond != 0) {
                ps.print("T");
            }
            if (hour != 0) {
                WriteValueTools.writeInt(hour, ps);
                ps.print("H");
            }
            if (minute != 0) {
                WriteValueTools.writeInt(minute, ps);
                ps.print("M");
            }
            if (second != 0 || millisecond != 0) {
                WriteValueTools.writeInt(second, ps);
            }
            if (millisecond > 0) {
                ps.print(".");
                WriteValueTools.writeInt(millisecond, ps);
            }
            if (second != 0 || millisecond != 0) {
                ps.print("S");
            }
            ps.print("\")");
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

}
