package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;



public class AFloatPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final AFloatPrinterFactory INSTANCE = new AFloatPrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return AFloatPrinter.INSTANCE;
    }

}