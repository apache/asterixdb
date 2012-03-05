package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.WriteValueTools;

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
        int seconds = AInt32SerializerDeserializer.getInt(b, s + 5);

        if (months < 0 || seconds < 0) {
            months *= -1;
            seconds *= -1;
            positive = false;
        }

        int month = (int) (months % 12);
        int year = (int) (months / 12);
        int second = (int) (seconds % 60);
        int minute = (int) (seconds % 3600 / 60);
        int hour = (int) (seconds % (86400) / 3600);
        int day = (int) (seconds / (86400));

        ps.print("duration(\"");
        if (!positive) {
            ps.print("-");
        }
        try {
            ps.print("D");
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
            ps.print("S\")");
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}