package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem.Fields;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ADatePrinter implements IPrinter {

    private static long CHRONON_OF_DAY = 24 * 60 * 60 * 1000;
    public static final ADatePrinter INSTANCE = new ADatePrinter();
    private static final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt32SerializerDeserializer.getInt(b, s + 1) * CHRONON_OF_DAY;

        ps.print("date(\"");
        try {
            gCalInstance.getExtendStringRepUntilField(chrononTime, 0, ps, Fields.YEAR, Fields.DAY, false);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        ps.print("\")");
    }

    public void printString(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt32SerializerDeserializer.getInt(b, s + 1) * CHRONON_OF_DAY;

        try {
            gCalInstance.getExtendStringRepUntilField(chrononTime, 0, ps, Fields.YEAR, Fields.DAY, false);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }
}