package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import java.io.PrintStream;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;

public class ADatePrinter implements IPrinter {

    private static final long serialVersionUID = 1L;
    public static final ADatePrinter INSTANCE = new ADatePrinter();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int year = AInt16SerializerDeserializer.getShort(b, s + 1) >> 1;
        int month = (AInt16SerializerDeserializer.getShort(b, s + 1) & 0x0001) * 8 + ((b[s + 3] >> 5) & 0x07);
        int day = b[s + 3] & 0x1f;
        byte timezone = b[s + 4];
        ps.format("date(\"" + (year < 0 ? "%+05d" : "%04d") + "-%02d-%02d%+03d:%02d\")", year, month, day,
                timezone / 4, timezone % 4 * ((timezone >= 0) ? 15 : -15));
    }
}