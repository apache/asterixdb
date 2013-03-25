package edu.uci.ics.asterix.dataflow.data.nontagged.printers.json;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class AInt8Printer implements IPrinter {

    public static final AInt8Printer INSTANCE = new AInt8Printer();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        byte o = AInt8SerializerDeserializer.getByte(b, s + 1);

        ps.println("{ int8: ");
        ps.println(o);
        ps.println("}");
    }
}