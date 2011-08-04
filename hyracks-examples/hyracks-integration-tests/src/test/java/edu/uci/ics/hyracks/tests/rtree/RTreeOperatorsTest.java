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

package edu.uci.ics.hyracks.tests.rtree;

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
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
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
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexStatsOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestTreeIndexRegistryProvider;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class RTreeOperatorsTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    private IStorageManagerInterface storageManager = new TestStorageManagerInterface();
    private IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider = new TestTreeIndexRegistryProvider();
    private ITreeIndexOpHelperFactory opHelperFactory = new RTreeOpHelperFactory();
    private ITreeIndexOpHelperFactory bTreeopHelperFactory = new BTreeOpHelperFactory();

    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    private final static String sep = System.getProperty("file.separator");

    // field, type and key declarations for primary R-tree index
    private int primaryFieldCount = 5;
    private int primaryKeyFieldCount = 4;
    private ITypeTrait[] primaryTypeTraits = new ITypeTrait[primaryFieldCount];
    private IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];

    private RTreeTypeAwareTupleWriterFactory primaryTupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
            primaryTypeTraits);
    private ITreeIndexFrameFactory primaryInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(
            primaryTupleWriterFactory, primaryKeyFieldCount);
    private ITreeIndexFrameFactory primaryLeafFrameFactory = new RTreeNSMLeafFrameFactory(primaryTupleWriterFactory,
            primaryKeyFieldCount);

    private static String primaryRTreeName = "primary" + simpleDateFormat.format(new Date());
    private static String primaryFileName = System.getProperty("java.io.tmpdir") + sep + primaryRTreeName;

    private IFileSplitProvider primaryRTreeSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(primaryFileName))) });

    private RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE });

    // field, type and key declarations for primary B-tree index
    private int primaryBTreeFieldCount = 10;
    private ITypeTrait[] primaryBTreeTypeTraits = new ITypeTrait[primaryBTreeFieldCount];
    private int primaryBTreeKeyFieldCount = 1;
    private IBinaryComparatorFactory[] primaryBTreeComparatorFactories = new IBinaryComparatorFactory[primaryBTreeKeyFieldCount];
    private TypeAwareTupleWriterFactory primaryBTreeTupleWriterFactory = new TypeAwareTupleWriterFactory(
            primaryBTreeTypeTraits);
    private ITreeIndexFrameFactory primaryBTreeInteriorFrameFactory = new BTreeNSMInteriorFrameFactory(
            primaryBTreeTupleWriterFactory);
    private ITreeIndexFrameFactory primaryBTreeLeafFrameFactory = new BTreeNSMLeafFrameFactory(
            primaryBTreeTupleWriterFactory);

    private static String primaryBTreeName = "primaryBTree" + simpleDateFormat.format(new Date());
    private static String primaryBTreeFileName = System.getProperty("java.io.tmpdir") + sep + primaryBTreeName;

    private IFileSplitProvider primaryBTreeSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(primaryBTreeFileName))) });

    private RecordDescriptor primaryBTreeRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE, });

    // field, type and key declarations for secondary indexes
    private int secondaryFieldCount = 5;
    private ITypeTrait[] secondaryTypeTraits = new ITypeTrait[secondaryFieldCount];
    private int secondaryKeyFieldCount = 4;
    private IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[secondaryKeyFieldCount];
    private RTreeTypeAwareTupleWriterFactory secondaryTupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
            secondaryTypeTraits);
    private ITreeIndexFrameFactory secondaryInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(
            secondaryTupleWriterFactory, secondaryKeyFieldCount);
    private ITreeIndexFrameFactory secondaryLeafFrameFactory = new RTreeNSMLeafFrameFactory(
            secondaryTupleWriterFactory, secondaryKeyFieldCount);

    private static String secondaryRTreeName = "secondary" + simpleDateFormat.format(new Date());
    private static String secondaryFileName = System.getProperty("java.io.tmpdir") + sep + secondaryRTreeName;

    private IFileSplitProvider secondaryRTreeSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(secondaryFileName))) });

    private RecordDescriptor secondaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE });

    @Before
    public void setup() {
        // field, type and key declarations for primary R-tree index
        primaryTypeTraits[0] = new TypeTrait(8);
        primaryTypeTraits[1] = new TypeTrait(8);
        primaryTypeTraits[2] = new TypeTrait(8);
        primaryTypeTraits[3] = new TypeTrait(8);
        primaryTypeTraits[4] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryComparatorFactories[0] = DoubleBinaryComparatorFactory.INSTANCE;
        primaryComparatorFactories[1] = primaryComparatorFactories[0];
        primaryComparatorFactories[2] = primaryComparatorFactories[0];
        primaryComparatorFactories[3] = primaryComparatorFactories[0];

        // field, type and key declarations for primary B-tree index
        primaryBTreeTypeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryBTreeTypeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryBTreeTypeTraits[2] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryBTreeTypeTraits[3] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryBTreeTypeTraits[4] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryBTreeTypeTraits[5] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryBTreeTypeTraits[6] = new TypeTrait(8);
        primaryBTreeTypeTraits[7] = new TypeTrait(8);
        primaryBTreeTypeTraits[8] = new TypeTrait(8);
        primaryBTreeTypeTraits[9] = new TypeTrait(8);
        primaryBTreeComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;

        // field, type and key declarations for secondary indexes
        secondaryTypeTraits[0] = new TypeTrait(8);
        secondaryTypeTraits[1] = new TypeTrait(8);
        secondaryTypeTraits[2] = new TypeTrait(8);
        secondaryTypeTraits[3] = new TypeTrait(8);
        secondaryTypeTraits[4] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        secondaryComparatorFactories[0] = DoubleBinaryComparatorFactory.INSTANCE;
        secondaryComparatorFactories[1] = secondaryComparatorFactories[0];
        secondaryComparatorFactories[2] = secondaryComparatorFactories[0];
        secondaryComparatorFactories[3] = secondaryComparatorFactories[0];
    }

    @Test
    public void loadPrimaryBTreeIndexTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/orders-with-locations.txt"))) };
        IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 1000, new int[] { 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        int[] fieldPermutation = { 0, 1, 2, 4, 5, 7, 9, 10, 11, 12 };
        TreeIndexBulkLoadOperatorDescriptor primaryBTreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, treeIndexRegistryProvider, primaryBTreeSplitProvider, primaryBTreeInteriorFrameFactory,
                primaryBTreeLeafFrameFactory, primaryBTreeTypeTraits, primaryBTreeComparatorFactories,
                fieldPermutation, 0.7f, bTreeopHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBTreeBulkLoad, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, primaryBTreeBulkLoad, 0);

        spec.addRoot(primaryBTreeBulkLoad);
        runTest(spec);
    }

    @Test
    public void loadPrimaryIndexTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] objectsSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/spatial.txt"))) };
        IFileSplitProvider objectsSplitProvider = new ConstantFileSplitProvider(objectsSplits);
        RecordDescriptor objectsDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor objScanner = new FileScanOperatorDescriptor(spec, objectsSplitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { DoubleParserFactory.INSTANCE,
                        DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE, DoubleParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), objectsDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, objScanner, NC1_ID);

        int[] fieldPermutation = { 0, 1, 2, 3, 4 };
        TreeIndexBulkLoadOperatorDescriptor primaryRTreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, treeIndexRegistryProvider, primaryRTreeSplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, fieldPermutation, 0.7f,
                opHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryRTreeBulkLoad, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), objScanner, 0, primaryRTreeBulkLoad, 0);

        spec.addRoot(primaryRTreeBulkLoad);
        runTest(spec);
    }

    @Test
    public void loadSecondaryIndexTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // build dummy tuple containing nothing
        ArrayTupleBuilder tb = new ArrayTupleBuilder(primaryKeyFieldCount * 2);
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize("0", dos);
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);

        int[] lowKeyFields = null; // - infinity
        int[] highKeyFields = null; // + infinity

        // scan primary index
        BTreeSearchOperatorDescriptor primaryBTreeSearchOp = new BTreeSearchOperatorDescriptor(spec,
                primaryBTreeRecDesc, storageManager, treeIndexRegistryProvider, primaryBTreeSplitProvider,
                primaryBTreeInteriorFrameFactory, primaryBTreeLeafFrameFactory, primaryBTreeTypeTraits,
                primaryBTreeComparatorFactories, true, lowKeyFields, highKeyFields, true, true, opHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBTreeSearchOp, NC1_ID);

        // load secondary index
        int[] fieldPermutation = { 6, 7, 8, 9, 0 };
        TreeIndexBulkLoadOperatorDescriptor secondaryRTreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, treeIndexRegistryProvider, secondaryRTreeSplitProvider, secondaryInteriorFrameFactory,
                secondaryLeafFrameFactory, secondaryTypeTraits, secondaryComparatorFactories, fieldPermutation, 0.7f,
                opHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryRTreeBulkLoad, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryBTreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryBTreeSearchOp, 0, secondaryRTreeBulkLoad, 0);

        spec.addRoot(secondaryRTreeBulkLoad);
        runTest(spec);
    }

    @Test
    public void showPrimaryIndexStats() throws Exception {
        JobSpecification spec = new JobSpecification();

        TreeIndexStatsOperatorDescriptor primaryStatsOp = new TreeIndexStatsOperatorDescriptor(spec, storageManager,
                treeIndexRegistryProvider, primaryRTreeSplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, opHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryStatsOp, NC1_ID);

        spec.addRoot(primaryStatsOp);
        runTest(spec);
    }

    @Test
    public void searchPrimaryIndexTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // build tuple
        ArrayTupleBuilder tb = new ArrayTupleBuilder(primaryKeyFieldCount);
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        DoubleSerializerDeserializer.INSTANCE.serialize(61.2894, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(-149.624, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(61.8894, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(-149.024, dos);
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);

        int[] keyFields = { 0, 1, 2, 3 };

        RTreeSearchOperatorDescriptor primaryRTreeSearchOp = new RTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                storageManager, treeIndexRegistryProvider, primaryRTreeSplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, keyFields, opHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryRTreeSearchOp, NC1_ID);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryRTreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryRTreeSearchOp, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void searchSecondaryIndexTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // build tuple
        ArrayTupleBuilder tb = new ArrayTupleBuilder(secondaryKeyFieldCount);
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        DoubleSerializerDeserializer.INSTANCE.serialize(61.2894, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(-149.624, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(61.8894, dos);
        tb.addFieldEndOffset();
        DoubleSerializerDeserializer.INSTANCE.serialize(-149.024, dos);
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                DoubleSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);

        int[] keyFields = { 0, 1, 2, 3 };

        RTreeSearchOperatorDescriptor secondaryRTreeSearchOp = new RTreeSearchOperatorDescriptor(spec,
                secondaryRecDesc, storageManager, treeIndexRegistryProvider, secondaryRTreeSplitProvider,
                secondaryInteriorFrameFactory, secondaryLeafFrameFactory, secondaryTypeTraits,
                secondaryComparatorFactories, keyFields, opHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryRTreeSearchOp, NC1_ID);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, secondaryRTreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryRTreeSearchOp, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        File primary = new File(primaryFileName);
        primary.deleteOnExit();

        File primaryBTree = new File(primaryBTreeFileName);
        primaryBTree.deleteOnExit();

        File secondary = new File(secondaryFileName);
        secondary.deleteOnExit();
    }
}