package edu.uci.ics.asterix.dataflow.data.nontagged.printers;

import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;



public class AInt32PrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    public static final AInt32PrinterFactory INSTANCE = new AInt32PrinterFactory();

    @Override
    public IPrinter createPrinter() {
        return AInt32Printer.INSTANCE;
    }

}