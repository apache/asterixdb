package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem.Fields;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ADateTimePrinter implements IPrinter {

    public static final ADateTimePrinter INSTANCE = new ADateTimePrinter();
    private static final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt64SerializerDeserializer.getLong(b, s + 1);

        ps.print("datetime(\"");

        try {
            gCalInstance.getExtendStringRepUntilField(chrononTime, 0, ps, Fields.YEAR, Fields.MILLISECOND, true);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }

        ps.print("\")");
    }

    public void printString(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt64SerializerDeserializer.getLong(b, s + 1);

        try {
            gCalInstance.getExtendStringRepUntilField(chrononTime, 0, ps, Fields.YEAR, Fields.MILLISECOND, true);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        
    }
}