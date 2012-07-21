package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ADateTimePrinter implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final ADateTimePrinter INSTANCE = new ADateTimePrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt64SerializerDeserializer.getLong(b, s + 1);

        GregorianCalendarSystem calendar = GregorianCalendarSystem.getInstance();

        int year = calendar.getYear(chrononTime);
        int month = calendar.getMonthOfYear(chrononTime, year);

        ps.print("datetime(\"");
        ps.append(String.format(year < 0 ? "%05d" : "%04d", year)).append("-").append(String.format("%02d", month))
                .append("-").append(String.format("%02d", calendar.getDayOfMonthYear(chrononTime, year, month)))
                .append("T").append(String.format("%02d", calendar.getHourOfDay(chrononTime))).append(":")
                .append(String.format("%02d", calendar.getMinOfHour(chrononTime))).append(":")
                .append(String.format("%02d", calendar.getSecOfMin(chrononTime))).append(".")
                .append(String.format("%03d", calendar.getMillisOfSec(chrononTime))).append("Z");
        ps.print("\")");
    }
}