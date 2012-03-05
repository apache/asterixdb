package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class ADoublePrinter implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final ADoublePrinter INSTANCE = new ADoublePrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        ps.print(ADoubleSerializerDeserializer.getDouble(b, s + 1) + "d");
    }
}