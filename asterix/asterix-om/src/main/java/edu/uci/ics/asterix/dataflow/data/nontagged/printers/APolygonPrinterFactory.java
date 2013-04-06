package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;

public class APolygonPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final APolygonPrinterFactory INSTANCE = new APolygonPrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return APolygonPrinter.INSTANCE;
    }

}