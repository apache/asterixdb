package edu.uci.ics.asterix.dataflow.data.nontagged.printers.json;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ADatePrinter implements IPrinter {

    private static long CHRONON_OF_DAY = 24 * 60 * 60 * 1000;
    public static final ADatePrinter INSTANCE = new ADatePrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt32SerializerDeserializer.getInt(b, s + 1) * CHRONON_OF_DAY;

        ps.print("{ date: ");
        ps.print(chrononTime);
        ps.print("}");
    }
}