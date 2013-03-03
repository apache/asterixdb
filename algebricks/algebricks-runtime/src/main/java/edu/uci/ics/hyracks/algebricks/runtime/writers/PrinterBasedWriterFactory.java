package edu.uci.ics.hyracks.algebricks.runtime.writers;

import java.io.PrintStream;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IAWriter;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.data.IPrinter;
import edu.uci.ics.hyracks.algebricks.data.IPrinterFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class PrinterBasedWriterFactory implements IAWriterFactory {

    private static final long serialVersionUID = 1L;

    public static final PrinterBasedWriterFactory INSTANCE = new PrinterBasedWriterFactory();

    public PrinterBasedWriterFactory() {
    }

    @Override
    public IAWriter createWriter(final int[] fields, final PrintStream printStream,
            final IPrinterFactory[] printerFactories, RecordDescriptor inputRecordDescriptor) {
        final IPrinter[] printers = new IPrinter[printerFactories.length];
        for (int i = 0; i < printerFactories.length; i++) {
            printers[i] = printerFactories[i].createPrinter();
        }

        return new IAWriter() {

            @Override
            public void init() throws AlgebricksException {
                for (int i = 0; i < printers.length; i++) {
                    printers[i].init();
                }
            }

            @Override
            public void printTuple(IFrameTupleAccessor tAccess, int tIdx) throws AlgebricksException {
                for (int i = 0; i < fields.length; i++) {
                    int fldStart = tAccess.getTupleStartOffset(tIdx) + tAccess.getFieldSlotsLength()
                            + tAccess.getFieldStartOffset(tIdx, fields[i]);
                    int fldLen = tAccess.getFieldLength(tIdx, fields[i]);
                    if (i > 0) {
                        printStream.print("; ");
                    }
                    printers[i].print(tAccess.getBuffer().array(), fldStart, fldLen, printStream);
                }
            }
        };
    }
}
