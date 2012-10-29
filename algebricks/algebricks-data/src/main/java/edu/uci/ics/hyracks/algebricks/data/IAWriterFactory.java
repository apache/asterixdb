package edu.uci.ics.hyracks.algebricks.data;

import java.io.PrintStream;
import java.io.Serializable;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public interface IAWriterFactory extends Serializable {
    public IAWriter createWriter(int[] fields, PrintStream ps, IPrinterFactory[] printerFactories,
            RecordDescriptor inputRecordDescriptor);
}
