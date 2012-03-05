package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;

public class APoint3DPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final APoint3DPrinterFactory INSTANCE = new APoint3DPrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return APoint3DPrinter.INSTANCE;
    }

}