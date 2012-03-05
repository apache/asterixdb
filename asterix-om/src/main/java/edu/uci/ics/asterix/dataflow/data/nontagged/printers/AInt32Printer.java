package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.WriteValueTools;

public class AInt32Printer implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final AInt32Printer INSTANCE = new AInt32Printer();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int d = AInt32SerializerDeserializer.getInt(b, s + 1);
        try {
            WriteValueTools.writeInt(d, ps);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}