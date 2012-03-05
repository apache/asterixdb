package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;

public class ADoublePrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final ADoublePrinterFactory INSTANCE = new ADoublePrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return ADoublePrinter.INSTANCE;
    }

}