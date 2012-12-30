package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ATimePrinter implements IPrinter {

    public static final ATimePrinter INSTANCE = new ATimePrinter();
    private static final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int time = AInt32SerializerDeserializer.getInt(b, s + 1);

        ps.print("time(\"");
        ps.append(String.format("%02d", gCalInstance.getHourOfDay(time))).append(":")
                .append(String.format("%02d", gCalInstance.getMinOfHour(time))).append(":")
                .append(String.format("%02d", gCalInstance.getSecOfMin(time))).append(".")
                .append(String.format("%03d", gCalInstance.getMillisOfSec(time))).append("Z");

        ps.print("\")");
    }

}