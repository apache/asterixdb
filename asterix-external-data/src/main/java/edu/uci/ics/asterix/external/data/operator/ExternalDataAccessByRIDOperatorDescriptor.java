package edu.uci.ics.asterix.external.data.operator;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.external.adapter.factory.IGenericDatasetAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.IControlledAdapter;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class ExternalDataAccessByRIDOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

	/**
	 * This operator is used to access external data residing in hdfs using record ids pushed in frame buffers
	 */
	private static final long serialVersionUID = 1L;
	private final Map<String, Object> adapterConfiguration;
	private final IAType atype;
	private IGenericDatasetAdapterFactory datasourceAdapterFactory;
	private IControlledAdapter adapter;
	private final HashMap<Integer, String> files;
	
	public ExternalDataAccessByRIDOperatorDescriptor(
			IOperatorDescriptorRegistry spec, Map<String, Object> arguments, IAType atype,
			RecordDescriptor outRecDesc,IGenericDatasetAdapterFactory dataSourceAdapterFactory, HashMap<Integer, String> files) {
		super(spec, 1, 1);
		this.atype = atype;
		this.adapterConfiguration = arguments;
		this.datasourceAdapterFactory = dataSourceAdapterFactory;
		this.recordDescriptors[0] = outRecDesc;
		this.files = files;
	}

	@Override
	public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) throws HyracksDataException {
		return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
			@Override
			public void open() throws HyracksDataException {
				//create the access by index adapter
				try {
					adapter = datasourceAdapterFactory.createAccessByRIDAdapter(adapterConfiguration, atype, files);
					adapter.initialize(ctx);
				} catch (Exception e) {
					throw new HyracksDataException("error during creation of external read by RID adapter", e);
				}
				writer.open();
			}

			@Override
			public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
				adapter.processNextFrame(buffer, writer);
			}

			@Override
			public void close() throws HyracksDataException {
				//close adapter and flush remaining frame if needed
				adapter.close(writer);
				//close writer
				writer.close();
			}

			@Override
			public void fail() throws HyracksDataException {
				writer.fail();
			}
		};	
	}
}