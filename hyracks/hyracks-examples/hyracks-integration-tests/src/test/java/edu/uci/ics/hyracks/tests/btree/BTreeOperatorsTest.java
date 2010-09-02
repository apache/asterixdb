package edu.uci.ics.hyracks.tests.btree;

import java.io.DataOutputStream;
import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.types.Int32Accessor;
import edu.uci.ics.hyracks.storage.am.btree.types.StringAccessor;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class BTreeOperatorsTest extends AbstractIntegrationTest {
	
	@Test
	public void bulkLoadTest() throws Exception {
		JobSpecification spec = new JobSpecification();
		
        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new File("data/tpch0.001/orders-part1.tbl")) };
        IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });
        
        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraint ordersPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID) });
        ordScanner.setPartitionConstraint(ordersPartitionConstraint);

        InMemorySortOperatorDescriptor sorter = new InMemorySortOperatorDescriptor(spec, new int[] { 1 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, ordersDesc);
        PartitionConstraint sortersPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID) });
        sorter.setPartitionConstraint(sortersPartitionConstraint);
        
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
		IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();        
		
		IFieldAccessor[] fields = new IFieldAccessor[3];
		fields[0] = new StringAccessor(); // key
		fields[1] = new StringAccessor(); // payload
		fields[2] = new StringAccessor(); // payload

		int keyLen = 1;
		IBinaryComparator[] cmps = new IBinaryComparator[keyLen];
		cmps[0] = UTF8StringBinaryComparatorFactory.INSTANCE.createBinaryComparator();
		MultiComparator cmp = new MultiComparator(cmps, fields);
		
		IBufferCacheProvider bufferCacheProvider = new BufferCacheProvider();
		IBTreeRegistryProvider btreeRegistryProvider = new BTreeRegistryProvider();
        int[] keyFields = { 1 };
        int[] payloadFields = { 4, 5 };
        int btreeFileId = 0;
        
		BTreeBulkLoadOperatorDescriptor btreeBulkLoad = new BTreeBulkLoadOperatorDescriptor(spec, ordersSplitProvider, ordersDesc, bufferCacheProvider, btreeRegistryProvider, btreeFileId, "/tmp/btreetest.bin", interiorFrameFactory, leafFrameFactory, cmp, keyFields, payloadFields, 0.7f);
		
		PartitionConstraint btreePartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
		btreeBulkLoad.setPartitionConstraint(btreePartitionConstraint);
				
        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);
        
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        
        spec.addRoot(btreeBulkLoad);
        runTest(spec);
        
        // try an ordered scan on the bulk-loaded btree
        BTree btree = btreeRegistryProvider.getBTreeRegistry().get(btreeFileId);
        IBTreeCursor scanCursor = new RangeSearchCursor(leafFrameFactory.getFrame());
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btree.search(scanCursor, nullPred, leafFrameFactory.getFrame(), interiorFrameFactory.getFrame());
        try {
        	while (scanCursor.hasNext()) {
        		scanCursor.next();
        		byte[] array = scanCursor.getPage().getBuffer().array();
        		int recOffset = scanCursor.getOffset();                
        		String rec = cmp.printRecord(array, recOffset);
        		System.out.println(rec);
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
        	scanCursor.close();
        }
	}
		
	
	@Test
	public void btreeSearchTest() throws Exception {
		JobSpecification spec = new JobSpecification();
		
		IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] {
				new FileSplit(NC2_ID, new File("data/words.txt")), new FileSplit(NC1_ID, new File("data/words.txt")) });
		
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
						
		int btreeFileId = 1;		
		BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(spec, splitProvider, recDesc, bufferCacheProvider, btreeRegistryProvider, btreeFileId, "/tmp/btreetest.bin", interiorFrameFactory, leafFrameFactory, cmp, rangePred);
		//BTreeDiskOrderScanOperatorDescriptor btreeSearchOp = new BTreeDiskOrderScanOperatorDescriptor(spec, splitProvider, recDesc, bufferCacheProvider, btreeRegistryProvider, 0, "/tmp/btreetest.bin", interiorFrameFactory, leafFrameFactory, cmp);
		
		PartitionConstraint btreePartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
		btreeSearchOp.setPartitionConstraint(btreePartitionConstraint);
		
		PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
		PartitionConstraint printerPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        printer.setPartitionConstraint(printerPartitionConstraint);
        
        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, btreeSearchOp, 0, printer, 0);
        
        spec.addRoot(printer);
        runTest(spec);
    }
}
