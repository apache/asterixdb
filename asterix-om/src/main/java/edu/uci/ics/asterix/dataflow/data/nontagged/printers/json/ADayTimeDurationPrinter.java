package edu.uci.ics.asterix.dataflow.data.nontagged.printers.json;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ADayTimeDurationPrinter implements IPrinter {

    public static final ADayTimeDurationPrinter INSTANCE = new ADayTimeDurationPrinter();

    @Override
    public void init() throws AlgebricksException {
        // TODO Auto-generated method stub

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long milliseconds = AInt64SerializerDeserializer.getLong(b, s + 1);

        ps.print("{ day-time-duration: ");
        ps.print(milliseconds);
        ps.print("}");
    }

}
