package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;


import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class AFloatPrinter implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final AFloatPrinter INSTANCE = new AFloatPrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        ps.print(AFloatSerializerDeserializer.getFloat(b, s + 1) + "f");
    }
}