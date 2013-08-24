package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;
import java.util.UUID;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;

public class AUUIDPrinter implements IPrinter {

    public static final AUUIDPrinter INSTANCE = new AUUIDPrinter();

    @Override
    public void init() throws AlgebricksException {
        // do nothing
    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long msb = Integer64SerializerDeserializer.getLong(b, s);
        long lsb = Integer64SerializerDeserializer.getLong(b, s + 8);
        UUID uuid = new UUID(msb, lsb);
        ps.print(uuid.toString());
    }

}
