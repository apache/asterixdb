package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.WriteValueTools;

public class ATimePrinter implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final ATimePrinter INSTANCE = new ATimePrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int time = AInt32SerializerDeserializer.getInt(b, s + 1);
        ps.print("time(\"");
        int timezone = time >> 24;
        time -= (timezone << 24);
        printTime(time, timezone, ps);
        ps.print("\")");
    }

    final static void printTime(int time, int timezone, PrintStream ps) throws AlgebricksException {
        int msec = time * 20 % 1000;
        int sec = time * 20 % 60000 / 1000;
        int min = time * 20 % 3600000 / 60000;
        int hour = time * 20 % 216000000 / 3600000;
        try {
            if (hour < 10) {
                ps.print("0");
            }
            WriteValueTools.writeInt(hour, ps);
            ps.print(":");
            if (min < 10) {
                ps.print("0");
            }
            WriteValueTools.writeInt(min, ps);
            ps.print(":");
            if (sec < 10) {
                ps.print("0");
            }
            WriteValueTools.writeInt(sec, ps);
            ps.print(":");
            if (msec < 100) {
                ps.print("0");
            }
            if (msec < 10) {
                ps.print("0");
            }
            WriteValueTools.writeInt(msec, ps);
            ps.print(timezone >= 0 ? "+" : "-");
            int t1 = Math.abs(timezone / 4);
            if (t1 < 10) {
                ps.print("0");
            }
            WriteValueTools.writeInt(t1, ps);
            ps.print(":");
            int t2 = timezone % 4 * ((timezone >= 0) ? 15 : -15);
            if (t2 < 10) {
                ps.print("0");
            }
            WriteValueTools.writeInt(t2, ps);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}