package edu.uci.ics.hyracks.storage.am.lsmtree.datagen;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

@SuppressWarnings({"rawtypes", "unchecked" })
public class TupleGenerator {    
    protected final ISerializerDeserializer[] fieldSerdes;
    protected final IFieldValueGenerator[] fieldGens;
    protected final ArrayTupleBuilder tb;
    protected final ArrayTupleReference tuple;
    protected final byte[] payload;
    protected final DataOutput tbDos;
    
    public TupleGenerator(IFieldValueGenerator[] fieldGens, ISerializerDeserializer[] fieldSerdes, int payloadSize) {
        this.fieldSerdes = fieldSerdes;
        this.fieldGens = fieldGens;
        tuple = new ArrayTupleReference();
        if (payloadSize > 0) {
            tb = new ArrayTupleBuilder(fieldSerdes.length + 1);
            payload = new byte[payloadSize];
        } else {
            tb = new ArrayTupleBuilder(fieldSerdes.length);
            payload = null;
        }        
        tbDos = tb.getDataOutput();
    }

    public ITupleReference next() throws IOException {
        tb.reset();
        for (int i = 0; i < fieldSerdes.length; i++) {
            fieldSerdes[i].serialize(fieldGens[i].next(), tbDos);
            tb.addFieldEndOffset();
        }
        if (payload != null) {
            tbDos.write(payload);
            tb.addFieldEndOffset();
        }
        tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
        return tuple;
    }
    
    public ITupleReference get() {
        return tuple;
    }
}
