package edu.uci.ics.hivesterix.runtime.jobgen;

import java.util.List;

import org.apache.hadoop.hive.ql.plan.PartitionDesc;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSource;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourcePropertiesProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;

public class HiveDataSource<P> implements IDataSource<P> {

	private P source;

	private Object[] schema;

	public HiveDataSource(P dataSource, Object[] sourceSchema) {
		source = dataSource;
		schema = sourceSchema;
	}

	@Override
	public P getId() {
		return source;
	}

	@Override
	public Object[] getSchemaTypes() {
		return schema;
	}

	@Override
	public void computeFDs(List<LogicalVariable> scanVariables,
			List<FunctionalDependency> fdList) {
	}

	@Override
	public IDataSourcePropertiesProvider getPropertiesProvider() {
		return new HiveDataSourcePartitioningProvider();
	}

	@Override
	public String toString() {
		PartitionDesc desc = (PartitionDesc) source;
		return desc.getTableName();
	}
}
