package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;

public class ARectanglePrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final ARectanglePrinterFactory INSTANCE = new ARectanglePrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return ARectanglePrinter.INSTANCE;
    }

}