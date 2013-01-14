package edu.uci.ics.hivesterix.runtime.provider;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.ISerializerDeserializerProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;

public class HiveSerializerDeserializerProvider implements
		ISerializerDeserializerProvider {

	public static final HiveSerializerDeserializerProvider INSTANCE = new HiveSerializerDeserializerProvider();

	private HiveSerializerDeserializerProvider() {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public ISerializerDeserializer getSerializerDeserializer(Object type)
			throws AlgebricksException {
		// return ARecordSerializerDeserializer.SCHEMALESS_INSTANCE;
		return null;
	}

}
