package edu.uci.ics.asterix.dataflow.data.nontagged;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class AqlNullWriterFactory implements INullWriterFactory {

    private static final long serialVersionUID = 1L;
    public static final AqlNullWriterFactory INSTANCE = new AqlNullWriterFactory();

    private AqlNullWriterFactory() {
    }

    @Override
    public INullWriter createNullWriter() {

        return new INullWriter() {

            @Override
            public void writeNull(DataOutput out) throws HyracksDataException {
                try {
                    out.writeByte(ATypeTag.NULL.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

}
