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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
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
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestTreeIndexRegistryProvider;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class BTreeSecondaryIndexSearchOperatorTest extends
		AbstractIntegrationTest {
	static {
		TestStorageManagerComponentHolder.init(8192, 20, 20);
	}

	private IStorageManagerInterface storageManager = new TestStorageManagerInterface();
	private IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider = new TestTreeIndexRegistryProvider();
	private ITreeIndexOpHelperFactory opHelperFactory = new BTreeOpHelperFactory();

	private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
			"ddMMyy-hhmmssSS");
	private final static String sep = System.getProperty("file.separator");

	// field, type and key declarations for primary index
	private int primaryFieldCount = 6;
	private ITypeTrait[] primaryTypeTraits = new ITypeTrait[primaryFieldCount];
	private int primaryKeyFieldCount = 1;
	private IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];
	private TypeAwareTupleWriterFactory primaryTupleWriterFactory = new TypeAwareTupleWriterFactory(
			primaryTypeTraits);
	private ITreeIndexFrameFactory primaryInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(
			primaryTupleWriterFactory, primaryComparatorFactories);
	private ITreeIndexFrameFactory primaryLeafFrameFactory = new BTreeNSMLeafFrameFactory(
			primaryTupleWriterFactory, primaryComparatorFactories);

	private static String primaryBtreeName = "primary"
			+ simpleDateFormat.format(new Date());
	private static String primaryFileName = System
			.getProperty("java.io.tmpdir") + sep + primaryBtreeName;

	private IFileSplitProvider primaryBtreeSplitProvider = new ConstantFileSplitProvider(
			new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
					primaryFileName))) });

	private RecordDescriptor primaryRecDesc = new RecordDescriptor(
			new ISerializerDeserializer[] {
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE });

	// field, type and key declarations for secondary indexes
	private int secondaryFieldCount = 2;
	private ITypeTrait[] secondaryTypeTraits = new ITypeTrait[secondaryFieldCount];
	private int secondaryKeyFieldCount = 2;
	private IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[secondaryKeyFieldCount];
	private TypeAwareTupleWriterFactory secondaryTupleWriterFactory = new TypeAwareTupleWriterFactory(
			secondaryTypeTraits);
	private ITreeIndexFrameFactory secondaryInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(
			secondaryTupleWriterFactory, secondaryComparatorFactories);
	private ITreeIndexFrameFactory secondaryLeafFrameFactory = new BTreeNSMLeafFrameFactory(
			secondaryTupleWriterFactory,secondaryComparatorFactories);

	private static String secondaryBtreeName = "secondary"
			+ simpleDateFormat.format(new Date());
	private static String secondaryFileName = System
			.getProperty("java.io.tmpdir") + sep + secondaryBtreeName;

	private IFileSplitProvider secondaryBtreeSplitProvider = new ConstantFileSplitProvider(
			new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
					secondaryFileName))) });

	private RecordDescriptor secondaryRecDesc = new RecordDescriptor(
			new ISerializerDeserializer[] {
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE });

	@Before
	public void setup() throws Exception {
		// field, type and key declarations for primary index
		primaryTypeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		primaryTypeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		primaryTypeTraits[2] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		primaryTypeTraits[3] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		primaryTypeTraits[4] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		primaryTypeTraits[5] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		primaryComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;

		// field, type and key declarations for secondary indexes
		secondaryTypeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		secondaryTypeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
		secondaryComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
		secondaryComparatorFactories[1] = UTF8StringBinaryComparatorFactory.INSTANCE;

		loadPrimaryIndexTest();
		loadSecondaryIndexTest();
	}

	public void loadPrimaryIndexTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC1_ID,
				new FileReference(new File("data/tpch0.001/orders-part1.tbl"))) };
		IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(
				ordersSplits);
		RecordDescriptor ordersDesc = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE });

		FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(
				spec, ordersSplitProvider, new DelimitedDataTupleParserFactory(
						new IValueParserFactory[] {
								UTF8StringParserFactory.INSTANCE,
								UTF8StringParserFactory.INSTANCE,
								UTF8StringParserFactory.INSTANCE,
								UTF8StringParserFactory.INSTANCE,
								UTF8StringParserFactory.INSTANCE,
								UTF8StringParserFactory.INSTANCE,
								UTF8StringParserFactory.INSTANCE,
								UTF8StringParserFactory.INSTANCE,
								UTF8StringParserFactory.INSTANCE }, '|'),
				ordersDesc);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				ordScanner, NC1_ID);

		ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(
				spec,
				1000,
				new int[] { 0 },
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				ordersDesc);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter,
				NC1_ID);

		int[] fieldPermutation = { 0, 1, 2, 4, 5, 7 };
		TreeIndexBulkLoadOperatorDescriptor primaryBtreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(
				spec, storageManager, treeIndexRegistryProvider,
				primaryBtreeSplitProvider, primaryInteriorFrameFactory,
				primaryLeafFrameFactory, primaryTypeTraits,
				primaryComparatorFactories, fieldPermutation, 0.7f,
				opHelperFactory);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				primaryBtreeBulkLoad, NC1_ID);

		spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0,
				sorter, 0);

		spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0,
				primaryBtreeBulkLoad, 0);

		spec.addRoot(primaryBtreeBulkLoad);
		runTest(spec);
	}

	public void loadSecondaryIndexTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		// build dummy tuple containing nothing
		ArrayTupleBuilder tb = new ArrayTupleBuilder(primaryKeyFieldCount * 2);
		DataOutput dos = tb.getDataOutput();

		tb.reset();
		UTF8StringSerializerDeserializer.INSTANCE.serialize("0", dos);
		tb.addFieldEndOffset();

		ISerializerDeserializer[] keyRecDescSers = {
				UTF8StringSerializerDeserializer.INSTANCE,
				UTF8StringSerializerDeserializer.INSTANCE };
		RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

		ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(
				spec, keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(),
				tb.getSize());
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				keyProviderOp, NC1_ID);

		int[] lowKeyFields = null; // - infinity
		int[] highKeyFields = null; // + infinity

		// scan primary index
		BTreeSearchOperatorDescriptor primaryBtreeSearchOp = new BTreeSearchOperatorDescriptor(
				spec, primaryRecDesc, storageManager,
				treeIndexRegistryProvider, primaryBtreeSplitProvider,
				primaryInteriorFrameFactory, primaryLeafFrameFactory,
				primaryTypeTraits, primaryComparatorFactories, true,
				lowKeyFields, highKeyFields, true, true, opHelperFactory);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				primaryBtreeSearchOp, NC1_ID);

		// sort based on secondary keys
		ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(
				spec,
				1000,
				new int[] { 3, 0 },
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				primaryRecDesc);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter,
				NC1_ID);

		// load secondary index
		int[] fieldPermutation = { 3, 0 };
		TreeIndexBulkLoadOperatorDescriptor secondaryBtreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(
				spec, storageManager, treeIndexRegistryProvider,
				secondaryBtreeSplitProvider, secondaryInteriorFrameFactory,
				secondaryLeafFrameFactory, secondaryTypeTraits,
				secondaryComparatorFactories, fieldPermutation, 0.7f,
				opHelperFactory);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				secondaryBtreeBulkLoad, NC1_ID);

		spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0,
				primaryBtreeSearchOp, 0);
		spec.connect(new OneToOneConnectorDescriptor(spec),
				primaryBtreeSearchOp, 0, sorter, 0);
		spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0,
				secondaryBtreeBulkLoad, 0);

		spec.addRoot(secondaryBtreeBulkLoad);
		runTest(spec);
	}

	@Test
	public void searchSecondaryIndexTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		// build tuple containing search keys (only use the first key as search
		// key)
		ArrayTupleBuilder tb = new ArrayTupleBuilder(secondaryKeyFieldCount);
		DataOutput dos = tb.getDataOutput();

		tb.reset();
		// low key
		UTF8StringSerializerDeserializer.INSTANCE.serialize("1998-07-21", dos);
		tb.addFieldEndOffset();
		// high key
		UTF8StringSerializerDeserializer.INSTANCE.serialize("2000-10-18", dos);
		tb.addFieldEndOffset();

		ISerializerDeserializer[] keyRecDescSers = {
				UTF8StringSerializerDeserializer.INSTANCE,
				UTF8StringSerializerDeserializer.INSTANCE };
		RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

		ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(
				spec, keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(),
				tb.getSize());
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				keyProviderOp, NC1_ID);

		int[] secondaryLowKeyFields = { 0 };
		int[] secondaryHighKeyFields = { 1 };

		// search secondary index
		BTreeSearchOperatorDescriptor secondaryBtreeSearchOp = new BTreeSearchOperatorDescriptor(
				spec, secondaryRecDesc, storageManager,
				treeIndexRegistryProvider, secondaryBtreeSplitProvider,
				secondaryInteriorFrameFactory, secondaryLeafFrameFactory,
				secondaryTypeTraits, secondaryComparatorFactories, true,
				secondaryLowKeyFields, secondaryHighKeyFields, true, true,
				opHelperFactory);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				secondaryBtreeSearchOp, NC1_ID);

		int[] primaryLowKeyFields = { 1 }; // second field from the tuples
		// coming from secondary index
		int[] primaryHighKeyFields = { 1 }; // second field from the tuples
		// coming from secondary index

		// search primary index
		BTreeSearchOperatorDescriptor primaryBtreeSearchOp = new BTreeSearchOperatorDescriptor(
				spec, primaryRecDesc, storageManager,
				treeIndexRegistryProvider, primaryBtreeSplitProvider,
				primaryInteriorFrameFactory, primaryLeafFrameFactory,
				primaryTypeTraits, primaryComparatorFactories, true,
				primaryLowKeyFields, primaryHighKeyFields, true, true,
				opHelperFactory);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				primaryBtreeSearchOp, NC1_ID);

		PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC1_ID);

		spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0,
				secondaryBtreeSearchOp, 0);
		spec.connect(new OneToOneConnectorDescriptor(spec),
				secondaryBtreeSearchOp, 0, primaryBtreeSearchOp, 0);
		spec.connect(new OneToOneConnectorDescriptor(spec),
				primaryBtreeSearchOp, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@AfterClass
	public static void cleanup() throws Exception {
		File primary = new File(primaryFileName);
		primary.deleteOnExit();
		File secondary = new File(secondaryFileName);
		secondary.deleteOnExit();
	}
}