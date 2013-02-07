package edu.uci.ics.hyracks.tests.util;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class NoopNullWriterFactory implements INullWriterFactory {

    private static final long serialVersionUID = 1L;
    public static final NoopNullWriterFactory INSTANCE = new NoopNullWriterFactory();

    private NoopNullWriterFactory() {
    }

    @Override
    public INullWriter createNullWriter() {
        return new INullWriter() {
            @Override
            public void writeNull(DataOutput out) throws HyracksDataException {
                try {
                    out.writeShort(0);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
