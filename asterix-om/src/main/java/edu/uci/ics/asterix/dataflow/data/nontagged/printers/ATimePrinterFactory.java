package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;



public class ATimePrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final ATimePrinterFactory INSTANCE = new ATimePrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return ATimePrinter.INSTANCE;
    }

}