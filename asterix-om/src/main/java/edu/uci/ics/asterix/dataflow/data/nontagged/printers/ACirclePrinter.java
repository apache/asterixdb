package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ACirclePrinter implements IPrinter {

    public static final ACirclePrinter INSTANCE = new ACirclePrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        ps.print("circle(\"");
        ps.print(ADoubleSerializerDeserializer.getDouble(b, s + 1));
        ps.print(",");
        ps.print(ADoubleSerializerDeserializer.getDouble(b, s + 9));
        ps.print(" ");
        ps.print(ADoubleSerializerDeserializer.getDouble(b, s + 17));
        ps.print("\")");
    }
}