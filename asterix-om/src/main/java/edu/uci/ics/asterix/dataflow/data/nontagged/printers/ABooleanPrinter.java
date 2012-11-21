package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ABooleanPrinter implements IPrinter {

    public static final ABooleanPrinter INSTANCE = new ABooleanPrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        ps.print(ABooleanSerializerDeserializer.getBoolean(b, s + 1));
    }
}