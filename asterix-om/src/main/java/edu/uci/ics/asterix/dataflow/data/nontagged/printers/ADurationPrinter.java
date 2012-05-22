package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.utils.WriteValueTools;

public class ADurationPrinter implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final ADurationPrinter INSTANCE = new ADurationPrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        boolean positive = true;
        int months = AInt32SerializerDeserializer.getInt(b, s + 1);
        long milliseconds = AInt64SerializerDeserializer.getLong(b, s + 5);

        if (months < 0 || milliseconds < 0) {
            months *= -1;
            milliseconds *= -1;
            positive = false;
        }

        int month = (int) (months % 12);
        int year = (int) (months / 12);
        int millisecond = (int) (milliseconds % 1000);
        int second = (int) (milliseconds % 60000 / 1000);
        int minute = (int) (milliseconds % 3600000 / 60000);
        int hour = (int) (milliseconds % (86400000) / 3600000);
        int day = (int) (milliseconds / (86400000));

        ps.print("duration(\"");
        if (!positive) {
            ps.print("-");
        }
        try {
            ps.print("P");
            WriteValueTools.writeInt(year, ps);
            ps.print("Y");
            WriteValueTools.writeInt(month, ps);
            ps.print("M");
            WriteValueTools.writeInt(day, ps);
            ps.print("DT");
            WriteValueTools.writeInt(hour, ps);
            ps.print("H");
            WriteValueTools.writeInt(minute, ps);
            ps.print("M");
            WriteValueTools.writeInt(second, ps);
            if (millisecond > 0) {
                ps.print(".");
                WriteValueTools.writeInt(millisecond, ps);
            }
            ps.print("S\")");
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}