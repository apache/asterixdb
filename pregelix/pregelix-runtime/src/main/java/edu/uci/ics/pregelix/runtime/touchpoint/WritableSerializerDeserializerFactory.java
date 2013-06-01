package edu.uci.ics.pregelix.runtime.touchpoint;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.pregelix.dataflow.std.base.ISerializerDeserializerFactory;

public class WritableSerializerDeserializerFactory<T extends Writable> implements ISerializerDeserializerFactory<T> {
    private static final long serialVersionUID = 1L;
    private final Class<T> clazz;

    public WritableSerializerDeserializerFactory(Class<T> clazz) {
        this.clazz = clazz;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public ISerializerDeserializer getSerializerDeserializer() {
        return DatatypeHelper.createSerializerDeserializer(clazz);
    }
}
