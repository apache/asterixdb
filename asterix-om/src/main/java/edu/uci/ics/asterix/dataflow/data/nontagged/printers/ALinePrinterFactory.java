package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;

public class ALinePrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final ALinePrinterFactory INSTANCE = new ALinePrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return ALinePrinter.INSTANCE;
    }

}