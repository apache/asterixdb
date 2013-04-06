package edu.uci.ics.asterix.dataflow.data.nontagged.printers.json;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class AInt32Printer implements IPrinter {

    public static final AInt32Printer INSTANCE = new AInt32Printer();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int d = AInt32SerializerDeserializer.getInt(b, s + 1);

        ps.println("{ int32: ");
        ps.println(d);
        ps.println("}");
    }
}