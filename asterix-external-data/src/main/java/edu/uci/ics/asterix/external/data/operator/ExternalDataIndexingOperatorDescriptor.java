package edu.uci.ics.asterix.external.data.operator;

import java.util.Map;

import edu.uci.ics.asterix.external.adapter.factory.IGenericDatasetAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/*
 * A single activity operator that provides the functionality of scanning data along 
 * with their RIDs using an instance of the configured adapter.
 */

public class ExternalDataIndexingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor{

	private static final long serialVersionUID = 1L;

	private final Map<String, Object> adapterConfiguration;
	private final Map<String,Integer> files;
	private final IAType atype;
	private IGenericDatasetAdapterFactory datasourceAdapterFactory;

	public ExternalDataIndexingOperatorDescriptor(JobSpecification spec, Map<String, Object> arguments, IAType atype,
			RecordDescriptor rDesc, IGenericDatasetAdapterFactory dataSourceAdapterFactory, Map<String,Integer> files) {
		super(spec, 0, 1);
		recordDescriptors[0] = rDesc;
		this.adapterConfiguration = arguments;
		this.atype = atype;
		this.datasourceAdapterFactory = dataSourceAdapterFactory;
		this.files = files;
	}

	@Override
	public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
					throws HyracksDataException {

		return new AbstractUnaryOutputSourceOperatorNodePushable() {
			@Override
			public void initialize() throws HyracksDataException {
				writer.open();
				IDatasourceAdapter adapter = null;
				try {
					adapter = ((IGenericDatasetAdapterFactory) datasourceAdapterFactory).createIndexingAdapter(
							adapterConfiguration, atype, files);
					adapter.initialize(ctx);
					adapter.start(partition, writer);
				} catch (Exception e) {
					throw new HyracksDataException("exception during reading from external data source", e);
				} finally {
					writer.close();
				}
			}
		};
	}
}