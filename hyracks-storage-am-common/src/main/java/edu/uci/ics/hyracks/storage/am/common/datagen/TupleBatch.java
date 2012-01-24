package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class TupleBatch {
    private final int size;
    private final TupleGenerator[] tupleGens;
    
    public TupleBatch(int size, IFieldValueGenerator[] fieldGens, ISerializerDeserializer[] fieldSerdes, int payloadSize) {        
        this.size = size;
        tupleGens = new TupleGenerator[size];
        for (int i = 0; i < size; i++) {
            tupleGens[i] = new TupleGenerator(fieldGens, fieldSerdes, payloadSize);
        }
    }
    
    public void generate() throws IOException {
        for(TupleGenerator tupleGen : tupleGens) {
            tupleGen.next();
        }
    }
    
    public int size() {
        return size;
    }
    
    public ITupleReference get(int ix) {
        return tupleGens[ix].get();
    }
}
