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

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
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
import edu.uci.ics.hyracks.storage.am.btree.dataflow.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.FileMappingProviderProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBufferCacheProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IFileMappingProviderProvider;
import edu.uci.ics.hyracks.storage.am.btree.frames.MetaDataFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOp;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.btree.impls.MultiComparator;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.FileHandle;
import edu.uci.ics.hyracks.storage.common.file.FileManager;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class BTreeOperatorsTest extends AbstractIntegrationTest {
	
	@Test
	public void bulkLoadTest() throws Exception {		
		// relies on the fact that NCs are run from same process
		//System.setProperty("NodeControllerDataPath", System.getProperty("java.io.tmpdir") + "/");
		
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
        
        // declare fields
		int fieldCount = 3;
		ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[2] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
				
        // declare keys
		int keyFieldCount = 1;
		IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keyFieldCount];
		comparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;		
		
		TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
		//SimpleTupleWriterFactory tupleWriterFactory = new SimpleTupleWriterFactory();
		IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory(tupleWriterFactory);
		IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory(tupleWriterFactory);		
		IBufferCacheProvider bufferCacheProvider = new BufferCacheProvider();		
		IBTreeRegistryProvider btreeRegistryProvider = new BTreeRegistryProvider();
		IFileMappingProviderProvider fileMappingProviderProvider = new FileMappingProviderProvider();
		
		int[] fieldPermutation = { 0, 4, 5};
		String btreeName = "btree.bin";
		String nc1FileName = System.getProperty("java.io.tmpdir") + "/nc1/" + btreeName;
		IFileSplitProvider btreeSplitProvider = new ConstantFileSplitProvider(
				new FileSplit[] { new FileSplit(NC1_ID, new File(nc1FileName)) } );
		
		BTreeBulkLoadOperatorDescriptor btreeBulkLoad = new BTreeBulkLoadOperatorDescriptor(spec, bufferCacheProvider, btreeRegistryProvider, btreeSplitProvider, fileMappingProviderProvider, interiorFrameFactory, leafFrameFactory, typeTraits, comparatorFactories, fieldPermutation, 0.7f);		
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
    	
        MultiComparator cmp = new MultiComparator(typeTraits, comparators);
        
        // try an ordered scan on the bulk-loaded btree
        int btreeFileId = 0; // TODO: this relies on the way FileMappingProvider assigns ids (in sequence starting from 0)
        BTree btree = btreeRegistryProvider.getBTreeRegistry().get(btreeFileId);
        IBTreeCursor scanCursor = new RangeSearchCursor(leafFrameFactory.getFrame());
        RangePredicate nullPred = new RangePredicate(true, null, null, true, true, null);
        BTreeOpContext opCtx = btree.createOpContext(BTreeOp.BTO_SEARCH, leafFrameFactory.getFrame(), interiorFrameFactory.getFrame(), null);
        btree.search(scanCursor, nullPred, opCtx);
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
		// relies on the fact that NCs are run from same process
		System.setProperty("NodeControllerDataPath", System.getProperty("java.io.tmpdir") + "/");
		
		JobSpecification spec = new JobSpecification();
						
		// declare fields
		int fieldCount = 3;
		ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[2] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		
        // declare keys
		int keyFieldCount = 1;
		IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[keyFieldCount];
		comparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;		
		
		TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
		//SimpleTupleWriterFactory tupleWriterFactory = new SimpleTupleWriterFactory();
		IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory(tupleWriterFactory);
		IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory(tupleWriterFactory);   				
		
		// construct a multicomparator from the factories (only for printing purposes)        
    	IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
    	for(int i = 0; i < comparatorFactories.length; i++) {
    		comparators[i] = comparatorFactories[i].createBinaryComparator();
    	}    	
        MultiComparator cmp = new MultiComparator(typeTraits, comparators);
		        
        // build tuple containing low and high search key
		ArrayTupleBuilder tb = new ArrayTupleBuilder(cmp.getKeyFieldCount()*2); // high key and low key
		DataOutput dos = tb.getDataOutput();
		
		tb.reset();		
		UTF8StringSerializerDeserializer.INSTANCE.serialize("100", dos); // low key
		tb.addFieldEndOffset();		
		UTF8StringSerializerDeserializer.INSTANCE.serialize("200", dos); // high key
		tb.addFieldEndOffset();		
    	
		ISerializerDeserializer[] keyRecDescSers = { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE };
		RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);
		
    	ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec, keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
		PartitionConstraint keyProviderPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
		keyProviderOp.setPartitionConstraint(keyProviderPartitionConstraint);
    	    	    	
		IBufferCacheProvider bufferCacheProvider = new BufferCacheProvider();
		IBTreeRegistryProvider btreeRegistryProvider = new BTreeRegistryProvider();		
		IFileMappingProviderProvider fileMappingProviderProvider = new FileMappingProviderProvider();
		
		RecordDescriptor recDesc = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });		
				
		String btreeName = "btree.bin";
		String nc1FileName = System.getProperty("java.io.tmpdir") + "/nc1/" + btreeName;
		IFileSplitProvider btreeSplitProvider = new ConstantFileSplitProvider(
				new FileSplit[] { new FileSplit(NC1_ID, new File(nc1FileName)) } );
		
		BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(spec, recDesc, bufferCacheProvider, btreeRegistryProvider, btreeSplitProvider, fileMappingProviderProvider, interiorFrameFactory, leafFrameFactory, typeTraits, comparatorFactories, true, new int[]{0}, new int[]{1}, true, true);
		//BTreeDiskOrderScanOperatorDescriptor btreeSearchOp = new BTreeDiskOrderScanOperatorDescriptor(spec, splitProvider, recDesc, bufferCacheProvider, btreeRegistryProvider, 0, "btreetest.bin", interiorFrameFactory, leafFrameFactory, cmp);
		
		PartitionConstraint btreePartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
		btreeSearchOp.setPartitionConstraint(btreePartitionConstraint);
		
		PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
		PartitionConstraint printerPartitionConstraint = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        printer.setPartitionConstraint(printerPartitionConstraint);
                
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, btreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), btreeSearchOp, 0, printer, 0);
        
        spec.addRoot(printer);
        runTest(spec);
    }
		
	@Test
	public void insertTest() throws Exception {
		// relies on the fact that NCs are run from same process
		System.setProperty("NodeControllerDataPath", System.getProperty("java.io.tmpdir") + "/");
		
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
                
		// we will create a primary index and 2 secondary indexes
		// first create comparators for primary index		
		int primaryFieldCount = 6;
		ITypeTrait[] primaryTypeTraits = new ITypeTrait[primaryFieldCount];
        primaryTypeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryTypeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryTypeTraits[2] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryTypeTraits[3] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryTypeTraits[4] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryTypeTraits[5] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        
		int primaryKeyFieldCount = 1;
		IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];
		primaryComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;		
		
		TypeAwareTupleWriterFactory primaryTupleWriterFactory = new TypeAwareTupleWriterFactory(primaryTypeTraits);
		//SimpleTupleWriterFactory primaryTupleWriterFactory = new SimpleTupleWriterFactory();
		IBTreeInteriorFrameFactory primaryInteriorFrameFactory = new NSMInteriorFrameFactory(primaryTupleWriterFactory);
		IBTreeLeafFrameFactory primaryLeafFrameFactory = new NSMLeafFrameFactory(primaryTupleWriterFactory);		
				
		IBufferCacheProvider bufferCacheProvider = new BufferCacheProvider();		
		IBTreeRegistryProvider btreeRegistryProvider = new BTreeRegistryProvider();		
		
		// trick to clear pages of old fileids
		BufferCacheProvider tmp = (BufferCacheProvider)bufferCacheProvider;
		tmp.reset();
				
		// construct a multicomparator for the primary index       
    	IBinaryComparator[] primaryComparators = new IBinaryComparator[primaryComparatorFactories.length];
    	for(int i = 0; i < primaryComparatorFactories.length; i++) {
    		primaryComparators[i] = primaryComparatorFactories[i].createBinaryComparator();
    	}
    	
        MultiComparator primaryCmp = new MultiComparator(primaryTypeTraits, primaryComparators);
                
        // now create comparators for secondary indexes		
		int secondaryFieldCount = 2;
		ITypeTrait[] secondaryTypeTraits = new ITypeTrait[secondaryFieldCount];
        secondaryTypeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        secondaryTypeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);        
		
		int secondaryKeyFieldCount = 2;
		IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[secondaryKeyFieldCount];
		secondaryComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
		secondaryComparatorFactories[1] = UTF8StringBinaryComparatorFactory.INSTANCE;		
		
		TypeAwareTupleWriterFactory secondaryTupleWriterFactory = new TypeAwareTupleWriterFactory(secondaryTypeTraits);
		//SimpleTupleWriterFactory secondaryTupleWriterFactory = new SimpleTupleWriterFactory();
		IBTreeInteriorFrameFactory secondaryInteriorFrameFactory = new NSMInteriorFrameFactory(secondaryTupleWriterFactory);
		IBTreeLeafFrameFactory secondaryLeafFrameFactory = new NSMLeafFrameFactory(secondaryTupleWriterFactory);		
		
		// construct a multicomparator for the secondary indexes        
    	IBinaryComparator[] secondaryComparators = new IBinaryComparator[secondaryComparatorFactories.length];
    	for(int i = 0; i < secondaryComparatorFactories.length; i++) {
    		secondaryComparators[i] = secondaryComparatorFactories[i].createBinaryComparator();
    	}
    	
        MultiComparator secondaryCmp = new MultiComparator(secondaryTypeTraits, secondaryComparators);
        
        // we create and register 3 btrees for in an insert pipeline being fed from a filescan op        
        IBufferCache bufferCache = bufferCacheProvider.getBufferCache();
        BTreeRegistry btreeRegistry = btreeRegistryProvider.getBTreeRegistry();        
        FileManager fileManager = bufferCacheProvider.getFileManager();
        IFileMappingProviderProvider fileMappingProviderProvider = new FileMappingProviderProvider();
        
        // primary index
        int fileIdA = fileMappingProviderProvider.getFileMappingProvider().mapNameToFileId("/tmp/btreetestA.ix", true);
        File fA = new File("/tmp/btreetestA.ix");
        RandomAccessFile rafA = new RandomAccessFile(fA, "rw");                
        FileHandle fiA = new FileHandle(fileIdA, rafA);
        fileManager.registerFile(fiA);
        BTree btreeA = new BTree(bufferCache, primaryInteriorFrameFactory, primaryLeafFrameFactory, primaryCmp);		
		btreeA.create(fileIdA, primaryLeafFrameFactory.getFrame(), new MetaDataFrame());
        btreeA.open(fileIdA);
        btreeRegistry.register(fileIdA, btreeA);
        
        // first secondary index
        int fileIdB = fileMappingProviderProvider.getFileMappingProvider().mapNameToFileId("/tmp/btreetestB.ix", true);
        File fB = new File("/tmp/btreetestB.ix");
        RandomAccessFile rafB = new RandomAccessFile(fB, "rw");   
        FileHandle fiB = new FileHandle(fileIdB, rafB);
        fileManager.registerFile(fiB);
        BTree btreeB = new BTree(bufferCache, secondaryInteriorFrameFactory, secondaryLeafFrameFactory, secondaryCmp);		
		btreeB.create(fileIdB, secondaryLeafFrameFactory.getFrame(), new MetaDataFrame());
        btreeB.open(fileIdB);
        btreeRegistry.register(fileIdB, btreeB);
        
        // second secondary index
        int fileIdC = fileMappingProviderProvider.getFileMappingProvider().mapNameToFileId("/tmp/btreetestC.ix", true);
        File fC = new File("/tmp/btreetestC.ix");
        RandomAccessFile rafC = new RandomAccessFile(fC, "rw");                
        FileHandle fiC = new FileHandle(fileIdC, rafC);
        fileManager.registerFile(fiC);
        BTree btreeC = new BTree(bufferCache, secondaryInteriorFrameFactory, secondaryLeafFrameFactory, secondaryCmp);	
		btreeC.create(fileIdC, secondaryLeafFrameFactory.getFrame(), new MetaDataFrame());
        btreeC.open(fileIdC);
        btreeRegistry.register(fileIdC, btreeC);
                        
        // create insert operators
        
        // primary index               
		IFileSplitProvider btreeSplitProviderA = new ConstantFileSplitProvider(
				new FileSplit[] { new FileSplit(NC1_ID, new File("/tmp/btreetestA.ix")) } );        
        int[] fieldPermutationA = { 0,1,2,3,4,5 };                       
        BTreeInsertUpdateDeleteOperatorDescriptor insertOpA = new BTreeInsertUpdateDeleteOperatorDescriptor(spec, ordersDesc, bufferCacheProvider, btreeRegistryProvider, btreeSplitProviderA, fileMappingProviderProvider, primaryInteriorFrameFactory, primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, fieldPermutationA, BTreeOp.BTO_INSERT);
        PartitionConstraint insertPartitionConstraintA = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        insertOpA.setPartitionConstraint(insertPartitionConstraintA);
        
        // first secondary index
        IFileSplitProvider btreeSplitProviderB = new ConstantFileSplitProvider(
				new FileSplit[] { new FileSplit(NC1_ID, new File("/tmp/btreetestB.ix")) } );   
        int[] fieldPermutationB = { 3, 0 };                    
        BTreeInsertUpdateDeleteOperatorDescriptor insertOpB = new BTreeInsertUpdateDeleteOperatorDescriptor(spec, ordersDesc, bufferCacheProvider, btreeRegistryProvider, btreeSplitProviderB, fileMappingProviderProvider, secondaryInteriorFrameFactory, secondaryLeafFrameFactory, secondaryTypeTraits, secondaryComparatorFactories, fieldPermutationB, BTreeOp.BTO_INSERT);
        PartitionConstraint insertPartitionConstraintB = new ExplicitPartitionConstraint(new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        insertOpB.setPartitionConstraint(insertPartitionConstraintB);
		
        // second secondary index
        IFileSplitProvider btreeSplitProviderC = new ConstantFileSplitProvider(
				new FileSplit[] { new FileSplit(NC1_ID, new File("/tmp/btreetestC.ix")) } );   
        int[] fieldPermutationC = { 4, 0 };                       
        BTreeInsertUpdateDeleteOperatorDescriptor insertOpC = new BTreeInsertUpdateDeleteOperatorDescriptor(spec, ordersDesc, bufferCacheProvider, btreeRegistryProvider, btreeSplitProviderC, fileMappingProviderProvider, secondaryInteriorFrameFactory, secondaryLeafFrameFactory, secondaryTypeTraits, secondaryComparatorFactories, fieldPermutationC, BTreeOp.BTO_INSERT);
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
        IBTreeCursor scanCursorA = new RangeSearchCursor(primaryLeafFrameFactory.getFrame());
        RangePredicate nullPredA = new RangePredicate(true, null, null, true, true, null);
        BTreeOpContext opCtxA = btreeA.createOpContext(BTreeOp.BTO_SEARCH, primaryLeafFrameFactory.getFrame(), primaryInteriorFrameFactory.getFrame(), null);
        btreeA.search(scanCursorA, nullPredA, opCtxA);
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
        IBTreeCursor scanCursorB = new RangeSearchCursor(secondaryLeafFrameFactory.getFrame());
        RangePredicate nullPredB = new RangePredicate(true, null, null, true, true, null);
        BTreeOpContext opCtxB = btreeB.createOpContext(BTreeOp.BTO_SEARCH, secondaryLeafFrameFactory.getFrame(), secondaryInteriorFrameFactory.getFrame(), null);
        btreeB.search(scanCursorB, nullPredB, opCtxB);
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
        IBTreeCursor scanCursorC = new RangeSearchCursor(secondaryLeafFrameFactory.getFrame());
        RangePredicate nullPredC = new RangePredicate(true, null, null, true, true, null);
        BTreeOpContext opCtxC = btreeC.createOpContext(BTreeOp.BTO_SEARCH, secondaryLeafFrameFactory.getFrame(), secondaryInteriorFrameFactory.getFrame(), null);
        btreeC.search(scanCursorC, nullPredC, opCtxC);
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
