package edu.uci.ics.asterix.dataflow.data.nontagged.printers.json;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class AInt16Printer implements IPrinter {
    public static final AInt16Printer INSTANCE = new AInt16Printer();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        short i = AInt16SerializerDeserializer.getShort(b, s + 1);

        ps.println("{ int16: ");
        ps.println(i);
        ps.println("}");
    }
}