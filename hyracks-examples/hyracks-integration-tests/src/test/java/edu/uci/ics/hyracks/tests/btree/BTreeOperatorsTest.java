/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.tests.btree;

import java.io.DataOutput;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.nc.runtime.HyracksContext;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
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
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeCursor;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeInsertUpdateDeleteOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeRegistry;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.FrameTupleReferenceFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.ITupleReferenceFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOp;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileInfo;
import edu.uci.ics.hyracks.storage.common.file.FileManager;
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

        InMemorySortOperatorDescriptor sorter = new InMemorySortOperatorDescriptor(spec, new int[] { 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, ordersDesc);
        PartitionConstraint sortersPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID) });
        sorter.setPartitionConstraint(sortersPartitionConstraint);
        
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
		IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
		
		IBufferCacheProvider bufferCacheProvider = new BufferCacheProvider();		
		IBTreeRegistryProvider btreeRegistryProvider = new BTreeRegistryProvider();
        		
		int fieldCount = 3;
		int keyFieldCount = 1;
		IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keyFieldCount];
		comparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;		
				
		int[] fieldPermutation = { 0, 4, 5 };  
        int btreeFileId = 0;
        
		BTreeBulkLoadOperatorDescriptor btreeBulkLoad = new BTreeBulkLoadOperatorDescriptor(spec, ordersSplitProvider, ordersDesc, bufferCacheProvider, btreeRegistryProvider, btreeFileId, "/tmp/btreetest.bin", interiorFrameFactory, leafFrameFactory, fieldCount, comparatorFactories, fieldPermutation, 0.7f, false);		
		PartitionConstraint btreePartitionConstraintA = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
		btreeBulkLoad.setPartitionConstraint(btreePartitionConstraintA);
				
        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);
        
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
              
        spec.addRoot(btreeBulkLoad);
        runTest(spec);
        
        // construct a multicomparator from the factories (only for printing purposes)        
    	IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
    	for(int i = 0; i < comparatorFactories.length; i++) {
    		comparators[i] = comparatorFactories[i].createBinaryComparator();
    	}
    	
        MultiComparator cmp = new MultiComparator(fieldCount, comparators);

        // try an ordered scan on the bulk-loaded btree
        BTree btreeA = btreeRegistryProvider.getBTreeRegistry().get(btreeFileId);
        IBTreeCursor scanCursor = new RangeSearchCursor(leafFrameFactory.getFrame());
        RangePredicate nullPred = new RangePredicate(true, null, null, null);
        btreeA.search(scanCursor, nullPred, leafFrameFactory.getFrame(), interiorFrameFactory.getFrame());
        try {
        	while (scanCursor.hasNext()) {
        		scanCursor.next();
        		ITupleReference frameTuple = scanCursor.getTuple();                                
                String rec = cmp.printTuple(frameTuple, ordersDesc.getFields());
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
				
		int fieldCount = 2;
		int keyFieldCount = 1;
		IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keyFieldCount];
		comparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;	
		
		// construct a multicomparator from the factories (only for printing purposes)        
    	IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
    	for(int i = 0; i < comparatorFactories.length; i++) {
    		comparators[i] = comparatorFactories[i].createBinaryComparator();
    	}
    	    	    	    	
        MultiComparator cmp = new MultiComparator(fieldCount, comparators);
		
        
        // put search keys into frame and create tuplereference factories
        IHyracksContext ctx = new HyracksContext(32768); // WARNING: make sure frame size is same as on NCs
        ByteBuffer keyFrame = ctx.getResourceManager().allocateFrame();
		FrameTupleAppender appender = new FrameTupleAppender(ctx);				
		appender.reset(keyFrame, true);
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getKeyFieldCount());
		DataOutput dos = tb.getDataOutput();
		
		ISerializerDeserializer[] keyRecDescSers = { IntegerSerializerDeserializer.INSTANCE};
		RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);
		
		// build low key
		tb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(-1000, dos);
    	tb.addFieldEndOffset();    	  
    	        	    	
    	appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
    	    	
    	// build high key
    	tb.reset();
    	IntegerSerializerDeserializer.INSTANCE.serialize(1000, dos);
    	tb.addFieldEndOffset();
    	
    	// build search key factories
    	ITupleReferenceFactory[] searchKeys = new ITupleReferenceFactory[2]; 
    	searchKeys[0] = new FrameTupleReferenceFactory(keyFrame.array(), 0, keyRecDesc);
    	searchKeys[1] = new FrameTupleReferenceFactory(keyFrame.array(), 1, keyRecDesc);
    	    	    	
		IBufferCacheProvider bufferCacheProvider = new BufferCacheProvider();
		IBTreeRegistryProvider btreeRegistryProvider = new BTreeRegistryProvider();
		
		RecordDescriptor recDesc = new RecordDescriptor(
                new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
		
		int btreeFileId = 2;		
		BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(spec, splitProvider, recDesc, bufferCacheProvider, btreeRegistryProvider, btreeFileId, "/tmp/btreetest.bin", interiorFrameFactory, leafFrameFactory, fieldCount, comparatorFactories, true, searchKeys, comparatorFactories.length, false);
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
	
	
	@Test
	public void insertTest() throws Exception {
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
        
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory();
		IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory();
		
		IBufferCacheProvider bufferCacheProvider = new BufferCacheProvider();		
		IBTreeRegistryProvider btreeRegistryProvider = new BTreeRegistryProvider();
		
		// we will create a primary index and 2 secondary indexes
		// first create comparators for primary index		
		int primaryFieldCount = 6;
		int primaryKeyFieldCount = 1;
		IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];
		primaryComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;		
		
		// construct a multicomparator for the primary index       
    	IBinaryComparator[] primaryComparators = new IBinaryComparator[primaryComparatorFactories.length];
    	for(int i = 0; i < primaryComparatorFactories.length; i++) {
    		primaryComparators[i] = primaryComparatorFactories[i].createBinaryComparator();
    	}
    	
        MultiComparator primaryCmp = new MultiComparator(primaryFieldCount, primaryComparators);
        
        
        // now create comparators for secondary indexes		
		int secondaryFieldCount = 2;
		int secondaryKeyFieldCount = 2;
		IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[secondaryKeyFieldCount];
		secondaryComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
		secondaryComparatorFactories[1] = UTF8StringBinaryComparatorFactory.INSTANCE;		
		
		// construct a multicomparator for the secondary indexes        
    	IBinaryComparator[] secondaryComparators = new IBinaryComparator[secondaryComparatorFactories.length];
    	for(int i = 0; i < secondaryComparatorFactories.length; i++) {
    		secondaryComparators[i] = secondaryComparatorFactories[i].createBinaryComparator();
    	}
    	
        MultiComparator secondaryCmp = new MultiComparator(secondaryFieldCount, secondaryComparators);
        
        // we create and register 3 btrees for in an insert pipeline being fed from a filescan op        
        IBufferCache bufferCache = bufferCacheProvider.getBufferCache();
        BTreeRegistry btreeRegistry = btreeRegistryProvider.getBTreeRegistry();        
        FileManager fileManager = bufferCacheProvider.getFileManager();
        
        // primary index
        int fileIdA = 3;
        File fA = new File("/tmp/btreetestA.ix");
        RandomAccessFile rafA = new RandomAccessFile(fA, "rw");                
        FileInfo fiA = new FileInfo(fileIdA, rafA);
        fileManager.registerFile(fiA);
        BTree btreeA = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, primaryCmp);		
		btreeA.create(fileIdA, leafFrameFactory.getFrame(), new MetaDataFrame());
        btreeA.open(fileIdA);
        btreeRegistry.register(fileIdA, btreeA);
        
        // first secondary index
        int fileIdB = 4;
        File fB = new File("/tmp/btreetestB.ix");
        RandomAccessFile rafB = new RandomAccessFile(fB, "rw");   
        FileInfo fiB = new FileInfo(fileIdB, rafB);
        fileManager.registerFile(fiB);
        BTree btreeB = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, secondaryCmp);		
		btreeB.create(fileIdB, leafFrameFactory.getFrame(), new MetaDataFrame());
        btreeB.open(fileIdB);
        btreeRegistry.register(fileIdB, btreeB);
        
        // second secondary index
        int fileIdC = 5;
        File fC = new File("/tmp/btreetestC.ix");
        RandomAccessFile rafC = new RandomAccessFile(fC, "rw");                
        FileInfo fiC = new FileInfo(fileIdC, rafC);
        fileManager.registerFile(fiC);
        BTree btreeC = new BTree(bufferCache, interiorFrameFactory, leafFrameFactory, secondaryCmp);	
		btreeC.create(fileIdC, leafFrameFactory.getFrame(), new MetaDataFrame());
        btreeC.open(fileIdC);
        btreeRegistry.register(fileIdC, btreeC);
        
                
        // create insert operators
        
        // primary index
        int[] fieldPermutationA = { 0,1,2,3,4,5 };                       
        BTreeInsertUpdateDeleteOperatorDescriptor insertOpA = new BTreeInsertUpdateDeleteOperatorDescriptor(spec, ordersSplitProvider, ordersDesc, bufferCacheProvider, btreeRegistryProvider, fileIdA, "/tmp/btreetestA.ix", interiorFrameFactory, leafFrameFactory, primaryFieldCount, primaryComparatorFactories, fieldPermutationA, BTreeOp.BTO_INSERT, true);
        PartitionConstraint insertPartitionConstraintA = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        insertOpA.setPartitionConstraint(insertPartitionConstraintA);
        
        // first secondary index
        int[] fieldPermutationB = { 3, 0 };                    
        BTreeInsertUpdateDeleteOperatorDescriptor insertOpB = new BTreeInsertUpdateDeleteOperatorDescriptor(spec, ordersSplitProvider, ordersDesc, bufferCacheProvider, btreeRegistryProvider, fileIdB, "/tmp/btreetestB.ix", interiorFrameFactory, leafFrameFactory, secondaryFieldCount, secondaryComparatorFactories, fieldPermutationB, BTreeOp.BTO_INSERT, true);
        PartitionConstraint insertPartitionConstraintB = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        insertOpB.setPartitionConstraint(insertPartitionConstraintB);
		
        // second secondary index
        int[] fieldPermutationC = { 4, 0 };                       
        BTreeInsertUpdateDeleteOperatorDescriptor insertOpC = new BTreeInsertUpdateDeleteOperatorDescriptor(spec, ordersSplitProvider, ordersDesc, bufferCacheProvider, btreeRegistryProvider, fileIdC, "/tmp/btreetestC.ix", interiorFrameFactory, leafFrameFactory, secondaryFieldCount, secondaryComparatorFactories, fieldPermutationC, BTreeOp.BTO_INSERT, true);
        PartitionConstraint insertPartitionConstraintC = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        insertOpC.setPartitionConstraint(insertPartitionConstraintC);
                
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        PartitionConstraint nullSinkPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        nullSink.setPartitionConstraint(nullSinkPartitionConstraint);
        
        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, insertOpA, 0);
        
        spec.connect(new OneToOneConnectorDescriptor(spec), insertOpA, 0, insertOpB, 0);
        
        spec.connect(new OneToOneConnectorDescriptor(spec), insertOpB, 0, insertOpC, 0);        
        
        spec.connect(new OneToOneConnectorDescriptor(spec), insertOpC, 0, nullSink, 0);               
        
        spec.addRoot(nullSink);
        runTest(spec);               
        
        // scan primary index         
        System.out.println("PRINTING PRIMARY INDEX");
        IBTreeCursor scanCursorA = new RangeSearchCursor(leafFrameFactory.getFrame());
        RangePredicate nullPredA = new RangePredicate(true, null, null, null);
        btreeA.search(scanCursorA, nullPredA, leafFrameFactory.getFrame(), interiorFrameFactory.getFrame());
        try {
        	while (scanCursorA.hasNext()) {
        		scanCursorA.next();
        		ITupleReference frameTuple = scanCursorA.getTuple();                                
                String rec = primaryCmp.printTuple(frameTuple, ordersDesc.getFields());
                System.out.println(rec);        		
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
        	scanCursorA.close();
        }            
        System.out.println();
        
        // scan first secondary index
        System.out.println("PRINTING FIRST SECONDARY INDEX");
        IBTreeCursor scanCursorB = new RangeSearchCursor(leafFrameFactory.getFrame());
        RangePredicate nullPredB = new RangePredicate(true, null, null, null);
        btreeB.search(scanCursorB, nullPredB, leafFrameFactory.getFrame(), interiorFrameFactory.getFrame());
        try {
        	while (scanCursorB.hasNext()) {
        		scanCursorB.next();
        		ITupleReference frameTuple = scanCursorB.getTuple();                                
                String rec = secondaryCmp.printTuple(frameTuple, ordersDesc.getFields());
                System.out.println(rec);   
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
        	scanCursorB.close();
        }           
        System.out.println();
        
        // scan second secondary index
        System.out.println("PRINTING SECOND SECONDARY INDEX");
        IBTreeCursor scanCursorC = new RangeSearchCursor(leafFrameFactory.getFrame());
        RangePredicate nullPredC = new RangePredicate(true, null, null, null);
        btreeC.search(scanCursorC, nullPredC, leafFrameFactory.getFrame(), interiorFrameFactory.getFrame());
        try {
        	while (scanCursorC.hasNext()) {
        		scanCursorC.next();
        		ITupleReference frameTuple = scanCursorC.getTuple();                                
                String rec = secondaryCmp.printTuple(frameTuple, ordersDesc.getFields());
                System.out.println(rec);
        	}
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
        	scanCursorC.close();
        }        
        System.out.println();
	}	
}
