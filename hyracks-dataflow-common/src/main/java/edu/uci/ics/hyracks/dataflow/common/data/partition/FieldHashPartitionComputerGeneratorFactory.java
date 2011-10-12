package edu.uci.ics.hyracks.dataflow.common.data.partition;


import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionGeneratorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerGeneratorFactory;



public class FieldHashPartitionComputerGeneratorFactory implements
		ITuplePartitionComputerGeneratorFactory {
	
	private static final long serialVersionUID = 1L;
    private final int[] hashFields;
    private final IBinaryHashFunctionGeneratorFactory[] hashFunctionGeneratorFactories;

    public FieldHashPartitionComputerGeneratorFactory(int[] hashFields, IBinaryHashFunctionGeneratorFactory[] hashFunctionGeneratorFactories) {
        this.hashFields = hashFields;
        this.hashFunctionGeneratorFactories = hashFunctionGeneratorFactories;
    }

	@Override
	public ITuplePartitionComputer createPartitioner(int seed) {
		final IBinaryHashFunction[] hashFunctions = new IBinaryHashFunction[hashFunctionGeneratorFactories.length];
        for (int i = 0; i < hashFunctionGeneratorFactories.length; ++i) {
            hashFunctions[i] = hashFunctionGeneratorFactories[i].createBinaryHashFunction(seed);
        }
        return new ITuplePartitionComputer() {
            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) {
                int h = 0;
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int slotLength = accessor.getFieldSlotsLength();
                for (int j = 0; j < hashFields.length; ++j) {
                    int fIdx = hashFields[j];
                    IBinaryHashFunction hashFn = hashFunctions[j];
                    int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
                    int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
                    int fh = hashFn
                            .hash(accessor.getBuffer().array(), startOffset + slotLength + fStart, fEnd - fStart);
                    h += fh;
                }
                if (h < 0) {
                    h = -h;
                }
                return h % nParts;
            }
        };
	}

	

}
