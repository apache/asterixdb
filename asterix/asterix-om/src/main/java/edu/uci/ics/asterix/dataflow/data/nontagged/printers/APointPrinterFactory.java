package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;


public class APointPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final APointPrinterFactory INSTANCE = new APointPrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return APointPrinter.INSTANCE;
    }

}