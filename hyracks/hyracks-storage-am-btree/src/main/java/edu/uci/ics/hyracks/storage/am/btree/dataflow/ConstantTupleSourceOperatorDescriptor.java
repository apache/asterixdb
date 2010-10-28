package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class ConstantTupleSourceOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
	
	private static final long serialVersionUID = 1L;
	
	private int[] fieldSlots;
	private byte[] tupleData;	
	private int tupleSize;
	
	public ConstantTupleSourceOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc, int[] fieldSlots, byte[] tupleData, int tupleSize) {
		super(spec, 0, 1);
		this.tupleData = tupleData;
		this.fieldSlots = fieldSlots;
		this.tupleSize = tupleSize;
		recordDescriptors[0] = recDesc;
	}
	
	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksContext ctx,
			IOperatorEnvironment env,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) throws HyracksDataException {
		return new ConstantTupleSourceOperatorNodePushable(ctx, fieldSlots, tupleData, tupleSize);
	}

}
