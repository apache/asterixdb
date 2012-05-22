package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ADatePrinter implements IPrinter {

    private static final long serialVersionUID = 1L;

    private static long CHRONON_OF_DAY = 24 * 60 * 60 * 1000;

    public static final ADatePrinter INSTANCE = new ADatePrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt32SerializerDeserializer.getInt(b, s + 1) * CHRONON_OF_DAY;

        GregorianCalendarSystem calendar = GregorianCalendarSystem.getInstance();

        int year = calendar.getYear(chrononTime);
        int month = calendar.getMonthOfYear(chrononTime, year);

        ps.print("date(\"");
        ps.append(String.format(year < 0 ? "%05d" : "%04d", year)).append("-").append(String.format("%02d", month))
                .append("-").append(String.format("%02d", calendar.getDayOfMonthYear(chrononTime, year, month)))
                .append("Z");
        ps.print("\")");
        //        int year = AInt16SerializerDeserializer.getShort(b, s + 1) >> 1;
        //        int month = (AInt16SerializerDeserializer.getShort(b, s + 1) & 0x0001) * 8 + ((b[s + 3] >> 5) & 0x07);
        //        int day = b[s + 3] & 0x1f;
        //        byte timezone = b[s + 4];
        //        ps.format("date(\"" + (year < 0 ? "%+05d" : "%04d") + "-%02d-%02d%+03d:%02d\")", year, month, day,
        //                timezone / 4, timezone % 4 * ((timezone >= 0) ? 15 : -15));
    }
}