package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;



public class AInt16PrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final AInt16PrinterFactory INSTANCE = new AInt16PrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return AInt16Printer.INSTANCE;
    }

}