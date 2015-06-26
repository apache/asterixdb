package edu.uci.ics.hyracks.dataflow.common.data.partition;

import java.util.Random;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class RandomPartitionComputerFactory implements
		ITuplePartitionComputerFactory {

	private static final long serialVersionUID = 1L;

	private final int domainCardinality;

	public RandomPartitionComputerFactory(int domainCardinality) {
		this.domainCardinality = domainCardinality;
	}

	@Override
	public ITuplePartitionComputer createPartitioner() {
		return new ITuplePartitionComputer() {

			private final Random random = new Random();

			@Override
			public int partition(IFrameTupleAccessor accessor, int tIndex,
					int nParts) throws HyracksDataException {
				return random.nextInt(domainCardinality);
			}
		};
	}

}
