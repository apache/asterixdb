package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;

public class ACirclePrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final ACirclePrinterFactory INSTANCE = new ACirclePrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return ACirclePrinter.INSTANCE;
    }

}