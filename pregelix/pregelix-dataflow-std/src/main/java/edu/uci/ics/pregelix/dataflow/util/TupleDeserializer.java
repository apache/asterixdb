package edu.uci.ics.pregelix.dataflow.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameConstants;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TupleDeserializer {
    private static final Logger LOGGER = Logger.getLogger(TupleDeserializer.class.getName());

    private Object[] record;
    private RecordDescriptor recordDescriptor;
    private ResetableByteArrayInputStream bbis;
    private DataInputStream di;

    public TupleDeserializer(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
        this.bbis = new ResetableByteArrayInputStream();
        this.di = new DataInputStream(bbis);
        this.record = new Object[recordDescriptor.getFields().length];
    }

    public Object[] deserializeRecord(ITupleReference tupleRef) throws HyracksDataException {
        for (int i = 0; i < record.length; ++i) {
            byte[] data = tupleRef.getFieldData(i);
            int offset = tupleRef.getFieldStart(i);
            bbis.setByteArray(data, offset);

            Object instance = recordDescriptor.getFields()[i].deserialize(di);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            record[i] = instance;
            if (FrameConstants.DEBUG_FRAME_IO) {
                try {
                    if (di.readInt() != FrameConstants.FRAME_FIELD_MAGIC) {
                        throw new HyracksDataException("Field magic mismatch");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return record;
    }

    public Object[] deserializeRecord(ArrayTupleBuilder tb) throws HyracksDataException {
        byte[] data = tb.getByteArray();
        int[] offset = tb.getFieldEndOffsets();
        int start = 0;
        for (int i = 0; i < record.length; ++i) {
            /**
             * reset the input
             */
            bbis.setByteArray(data, start);
            start = offset[i];

            /**
             * do deserialization
             */
            Object instance = recordDescriptor.getFields()[i].deserialize(di);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest(i + " " + instance);
            }
            record[i] = instance;
            if (FrameConstants.DEBUG_FRAME_IO) {
                try {
                    if (di.readInt() != FrameConstants.FRAME_FIELD_MAGIC) {
                        throw new HyracksDataException("Field magic mismatch");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return record;
    }
}
