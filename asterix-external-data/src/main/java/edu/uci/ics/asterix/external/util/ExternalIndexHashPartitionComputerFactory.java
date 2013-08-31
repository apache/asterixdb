package edu.uci.ics.asterix.external.util;

import java.io.DataInputStream;
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

/* A class that is used to partition external data tuples when building an index over them
 * the computer it returns, computes the HDFS block value before using the actual hash partitioning 
 * function. this way we ensure that records within ranges of 64MB sizes are partitioned together to the same
 * data node.
 */

public class ExternalIndexHashPartitionComputerFactory implements ITuplePartitionComputerFactory{
	private static final long serialVersionUID = 1L;
	private final int[] hashFields;
	private final int bytesInHDFSBlock = 67108864;
	private final IBinaryHashFunctionFactory[] hashFunctionFactories;
	@SuppressWarnings("unchecked")
	private final ISerializerDeserializer<AInt64> longSerde = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(BuiltinType.AINT64);

	public ExternalIndexHashPartitionComputerFactory(int[] hashFields, IBinaryHashFunctionFactory[] hashFunctionFactories) {
		this.hashFields = hashFields;
		this.hashFunctionFactories = hashFunctionFactories;
	}

	@Override
	public ITuplePartitionComputer createPartitioner() {
		final IBinaryHashFunction[] hashFunctions = new IBinaryHashFunction[hashFunctionFactories.length];
		for (int i = 0; i < hashFunctionFactories.length; ++i) {
			hashFunctions[i] = hashFunctionFactories[i].createBinaryHashFunction();
		}
		return new ITuplePartitionComputer() {
			private ByteBuffer serializedLong = ByteBuffer.allocate(8);;
			private AInt64 byteLocation;
			private ByteBufferInputStream bbis = new ByteBufferInputStream();
			private DataInputStream dis = new DataInputStream(bbis);
			@Override
			public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts){
				if (nParts == 1) {
					return 0;
				}
				int h = 0;
				int startOffset = accessor.getTupleStartOffset(tIndex);
				int slotLength = accessor.getFieldSlotsLength();
				for (int j = 0; j < hashFields.length; ++j) {
					int fIdx = hashFields[j];
					IBinaryHashFunction hashFn = hashFunctions[j];
					int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
					int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
					if(j == 1)
					{
						//reset the buffer
						serializedLong.clear();
						//read byte location
						bbis.setByteBuffer(accessor.getBuffer() , accessor.getTupleStartOffset(tIndex) + accessor.getFieldSlotsLength() + accessor.getFieldStartOffset(tIndex, hashFields[1]));
						try {
							byteLocation = ((AInt64) longSerde.deserialize(dis));
							//compute the block number, serialize it into a new array and call the hash function
							serializedLong.putLong(byteLocation.getLongValue()/bytesInHDFSBlock);
							//call the hash function
							int fh = hashFn
									.hash(serializedLong.array(), 0,serializedLong.capacity());
									h = h * 31 + fh;
						} catch (HyracksDataException e) {
							System.err.println("could not serialize the long byte position value!!!");
							e.printStackTrace();
						}
					}
					else
					{
						int fh = hashFn
						.hash(accessor.getBuffer().array(), startOffset + slotLength + fStart, fEnd - fStart);
						h = h * 31 + fh;
					}
				}
				if (h < 0) {
					h = -(h + 1);
				}
				return h % nParts;
			}
		};
	}
}