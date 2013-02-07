package edu.uci.ics.hivesterix.runtime.jobgen;

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;

public class HiveDataSink implements IDataSink {

	private Object[] schema;

	private Object fsOperator;

	public HiveDataSink(Object sink, Object[] sourceSchema) {
		schema = sourceSchema;
		fsOperator = sink;
	}

	@Override
	public Object getId() {
		return fsOperator;
	}

	@Override
	public Object[] getSchemaTypes() {
		return schema;
	}

	public IPartitioningProperty getPartitioningProperty() {
		return new RandomPartitioningProperty(new HiveDomain());
	}

}
