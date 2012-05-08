package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;

public class ADatePrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final ADatePrinterFactory INSTANCE = new ADatePrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return ADatePrinter.INSTANCE;
    }

}