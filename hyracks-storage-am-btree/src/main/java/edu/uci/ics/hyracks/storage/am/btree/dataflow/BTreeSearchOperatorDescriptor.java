package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.DataOutputStream;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.types.Int32Accessor;

public class BTreeSearchOperatorDescriptor extends AbstractBTreeOperatorDescriptor {

	private static final long serialVersionUID = 1L;

	public BTreeSearchOperatorDescriptor(JobSpecification spec, IFileSplitProvider fileSplitProvider, RecordDescriptor recDesc, IBufferCacheProvider bufferCacheProvider, IBTreeRegistryProvider btreeRegistryProvider,  int btreeFileId, String btreeFileName, IBTreeInteriorFrameFactory interiorFactory, IBTreeLeafFrameFactory leafFactory, MultiComparator cmp, RangePredicate rangePred) {
		super(spec, 0, 1, fileSplitProvider, recDesc, bufferCacheProvider, btreeRegistryProvider, btreeFileId, btreeFileName, interiorFactory, leafFactory, cmp, rangePred);        
	}

	@Override
	public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
			IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
		return new BTreeSearchOperatorNodePushable(this, ctx);
	}
	
	public static void main(String args[]) throws HyracksDataException {
		
		IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
		IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();        
		
		IFieldAccessor[] fields = new IFieldAccessor[2];
		fields[0] = new Int32Accessor(); // key field
		fields[1] = new Int32Accessor(); // value field
		
		int keyLen = 1;
		IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
		cmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
		MultiComparator cmp = new MultiComparator(cmps, fields);		

		ByteArrayAccessibleOutputStream lkbaaos = new ByteArrayAccessibleOutputStream();
		DataOutputStream lkdos = new DataOutputStream(lkbaaos);    	    	    	
		IntegerSerializerDeserializer.INSTANCE.serialize(-1000, lkdos);

		ByteArrayAccessibleOutputStream hkbaaos = new ByteArrayAccessibleOutputStream();
		DataOutputStream hkdos = new DataOutputStream(hkbaaos);    	    	    	
		IntegerSerializerDeserializer.INSTANCE.serialize(1000, hkdos);

		byte[] lowKey = lkbaaos.toByteArray();
		byte[] highKey = hkbaaos.toByteArray();

		IBinaryComparator[] searchCmps = new IBinaryComparator[1];
		searchCmps[0] = IntegerBinaryComparatorFactory.INSTANCE.createBinaryComparator();
		MultiComparator searchCmp = new MultiComparator(searchCmps, fields);

		RangePredicate rangePred = new RangePredicate(true, lowKey, highKey, searchCmp);

		IBufferCacheProvider bufferCacheProvider = new BufferCacheProvider();
		IBTreeRegistryProvider btreeRegistryProvider = new BTreeRegistryProvider();

		RecordDescriptor recDesc = new RecordDescriptor(
				new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

		JobSpecification spec = new JobSpecification();
		BTreeSearchOperatorDescriptor opDesc = new BTreeSearchOperatorDescriptor(spec, null, recDesc, bufferCacheProvider, btreeRegistryProvider, 0, "/tmp/btreetest.bin", interiorFrameFactory, leafFrameFactory, cmp, rangePred);		 		 
		IOperatorNodePushable op = opDesc.createPushRuntime(null, null, null, 0, 0);
		op.open();		 
	}

}
