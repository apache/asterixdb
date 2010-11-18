package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IBinaryTokenizerFactory;

public class BinaryTokenizerOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
	
	private static final long serialVersionUID = 1L;
	
	private final IBinaryTokenizerFactory tokenizerFactory;
	// fields that will be tokenized
	private final int[] tokenFields;
	// operator will emit these projected fields for each token, e.g., as payload for an inverted list
	// WARNING: too many projected fields can cause significant data blowup
	private final int[] projFields;
	
	public BinaryTokenizerOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc, IBinaryTokenizerFactory tokenizerFactory, int[] tokenFields, int[] projFields) {
		super(spec, 1, 1);
		this.tokenizerFactory = tokenizerFactory;
		this.tokenFields = tokenFields;
		this.projFields = projFields;
		recordDescriptors[0] = recDesc;
	}
	
	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksContext ctx,
			IOperatorEnvironment env,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) throws HyracksDataException {
		return new BinaryTokenizerOperatorNodePushable(ctx, 
				recordDescProvider.getInputRecordDescriptor(odId, 0), 
				recordDescriptors[0], tokenizerFactory.createBinaryTokenizer(), 
				tokenFields, projFields);
	}
}
