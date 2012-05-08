package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.utils.WriteValueTools;

public class ADateTimePrinter implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final ADateTimePrinter INSTANCE = new ADateTimePrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int year = AInt16SerializerDeserializer.getShort(b, s + 1) >> 1;
        int month = (AInt16SerializerDeserializer.getShort(b, s + 1) & 0x0001) * 8 + ((b[s + 3] >> 5) & 0x07);

        int day = b[s + 3] & 0x1f;
        byte timezone = b[s + 4];

        int time = AInt32SerializerDeserializer.getInt(b, s + 5);
        // int msec = (time) * 20 % 1000;
        // int sec = (time) * 20 % 60000 / 1000;
        // int min = (time) * 20 % 3600000 / 60000;
        // int hour = (time) * 20 % 216000000 / 3600000;

        ps.print("datetime(\"");
        if (year < 0) {
            ps.print("-");
        }
        int y = Math.abs(year);
        if (y < 1000) {
            ps.print("0");
        }
        if (y < 100) {
            ps.print("0");
        }
        if (y < 10) {
            ps.print("0");
        }
        try {
            WriteValueTools.writeInt(y, ps);
            ps.print("-");
            if (month < 10) {
                ps.print("0");
            }
            WriteValueTools.writeInt(month, ps);
            ps.print("-");
            if (day < 10) {
                ps.print("0");
            }
            WriteValueTools.writeInt(day, ps);
            ps.print("T");
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        ATimePrinter.printTime(time, timezone, ps);
        ps.print("\")");
    }
}