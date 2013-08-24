package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;

public class AUUIDPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;

    public static final AUUIDPrinterFactory INSTANCE = new AUUIDPrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return AUUIDPrinter.INSTANCE;
    }

}
