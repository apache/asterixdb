package edu.uci.ics.hyracks.dataflow.common.data.partition;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class RepartitionComputerGeneratorFactory implements ITuplePartitionComputerFamily{
	
	 private static final long serialVersionUID = 1L;

	    private int factor;
	    private ITuplePartitionComputerFamily delegateFactory;

	    public RepartitionComputerGeneratorFactory(int factor, ITuplePartitionComputerFamily delegate) {
	        this.factor = factor;
	        this.delegateFactory = delegate;
	    }

	@Override
	public ITuplePartitionComputer createPartitioner(int seed) {
		final int s = seed;
		return new ITuplePartitionComputer() {
            private ITuplePartitionComputer delegate = delegateFactory.createPartitioner(s);

            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
            	return delegate.partition(accessor, tIndex, factor * nParts) / factor;
            }
        };
	}

}
