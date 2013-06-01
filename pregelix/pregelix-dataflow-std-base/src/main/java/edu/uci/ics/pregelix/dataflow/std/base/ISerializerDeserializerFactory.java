package edu.uci.ics.pregelix.dataflow.std.base;

import java.io.Serializable;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;

public interface ISerializerDeserializerFactory<T> extends Serializable {

	public ISerializerDeserializer<T> getSerializerDeserializer();

}
