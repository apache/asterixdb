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
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
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
import edu.uci.ics.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestIndexRegistryProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestStorageManagerInterface;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;
import edu.uci.ics.hyracks.tests.util.ResultSerializerFactoryProvider;

public class RTreeSecondaryIndexSearchOperatorTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    private IStorageManagerInterface storageManager = new TestStorageManagerInterface();
    private IIndexRegistryProvider<IIndex> indexRegistryProvider = new TestIndexRegistryProvider();
    private IIndexDataflowHelperFactory dataflowHelperFactory;
    private IIndexDataflowHelperFactory btreeDataflowHelperFactory = new BTreeDataflowHelperFactory();

    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    private final static String sep = System.getProperty("file.separator");

    // field, type and key declarations for primary B-tree index
    private int primaryBTreeFieldCount = 10;
    private ITypeTraits[] primaryBTreeTypeTraits = new ITypeTraits[primaryBTreeFieldCount];
    private int primaryBTreeKeyFieldCount = 1;
    private IBinaryComparatorFactory[] primaryBTreeComparatorFactories = new IBinaryComparatorFactory[primaryBTreeKeyFieldCount];

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
    private ITypeTraits[] secondaryTypeTraits = new ITypeTraits[secondaryFieldCount];
    private int secondaryKeyFieldCount = 4;
    private IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[secondaryKeyFieldCount];

    private static String secondaryRTreeName = "secondary" + simpleDateFormat.format(new Date());
    private static String secondaryFileName = System.getProperty("java.io.tmpdir") + sep + secondaryRTreeName;

    private IFileSplitProvider secondaryRTreeSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(secondaryFileName))) });

    private RecordDescriptor secondaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE });

    private IPrimitiveValueProviderFactory[] secondaryValueProviderFactories;
    
    @Before
    public void setup() throws Exception {
        // field, type and key declarations for primary B-tree index
        primaryBTreeTypeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[2] = UTF8StringPointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[3] = UTF8StringPointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[4] = UTF8StringPointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[5] = UTF8StringPointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[6] = DoublePointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[7] = DoublePointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[8] = DoublePointable.TYPE_TRAITS;
        primaryBTreeTypeTraits[9] = DoublePointable.TYPE_TRAITS;
        primaryBTreeComparatorFactories[0] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);

        // field, type and key declarations for secondary indexes
        secondaryTypeTraits[0] = DoublePointable.TYPE_TRAITS;
        secondaryTypeTraits[1] = DoublePointable.TYPE_TRAITS;
        secondaryTypeTraits[2] = DoublePointable.TYPE_TRAITS;
        secondaryTypeTraits[3] = DoublePointable.TYPE_TRAITS;
        secondaryTypeTraits[4] = UTF8StringPointable.TYPE_TRAITS;
        secondaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        secondaryComparatorFactories[1] = secondaryComparatorFactories[0];
        secondaryComparatorFactories[2] = secondaryComparatorFactories[0];
        secondaryComparatorFactories[3] = secondaryComparatorFactories[0];

        secondaryValueProviderFactories = RTreeUtils
                .createPrimitiveValueProviderFactories(secondaryComparatorFactories.length, DoublePointable.FACTORY);

        dataflowHelperFactory = new RTreeDataflowHelperFactory(secondaryValueProviderFactories);
        
        createPrimaryIndex();
        loadPrimaryBTreeIndexTest();
        createSecondaryIndex();
        loadSecondaryIndexTest();
    }

    public void createPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        TreeIndexCreateOperatorDescriptor primaryCreateOp = new TreeIndexCreateOperatorDescriptor(spec, storageManager,
                indexRegistryProvider, primaryBTreeSplitProvider, primaryBTreeTypeTraits,
                primaryBTreeComparatorFactories, btreeDataflowHelperFactory, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryCreateOp, NC1_ID);
        spec.addRoot(primaryCreateOp);
        runTest(spec);
    }
    
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
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        int[] fieldPermutation = { 0, 1, 2, 4, 5, 7, 9, 10, 11, 12 };
        TreeIndexBulkLoadOperatorDescriptor primaryBTreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, indexRegistryProvider, primaryBTreeSplitProvider, primaryBTreeTypeTraits, primaryBTreeComparatorFactories,
                fieldPermutation, 0.7f, btreeDataflowHelperFactory, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBTreeBulkLoad, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, primaryBTreeBulkLoad, 0);

        spec.addRoot(primaryBTreeBulkLoad);
        runTest(spec);
    }

    public void createSecondaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        TreeIndexCreateOperatorDescriptor primaryCreateOp = new TreeIndexCreateOperatorDescriptor(spec, storageManager,
                indexRegistryProvider, secondaryRTreeSplitProvider, secondaryTypeTraits, secondaryComparatorFactories,
                dataflowHelperFactory, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryCreateOp, NC1_ID);
        spec.addRoot(primaryCreateOp);
        runTest(spec);
    }
    
    public void loadSecondaryIndexTest() throws Exception {
        JobSpecification spec = new JobSpecification();

        // build dummy tuple containing nothing
        ArrayTupleBuilder tb = new ArrayTupleBuilder(primaryBTreeKeyFieldCount * 2);
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
                primaryBTreeRecDesc, storageManager, indexRegistryProvider, primaryBTreeSplitProvider,
                primaryBTreeTypeTraits, primaryBTreeComparatorFactories, lowKeyFields, highKeyFields, 
                true, true, btreeDataflowHelperFactory, false, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBTreeSearchOp, NC1_ID);

        // load secondary index
        int[] fieldPermutation = { 6, 7, 8, 9, 0 };
        TreeIndexBulkLoadOperatorDescriptor secondaryRTreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, indexRegistryProvider, secondaryRTreeSplitProvider, secondaryTypeTraits, secondaryComparatorFactories, fieldPermutation, 0.7f,
                dataflowHelperFactory, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryRTreeBulkLoad, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryBTreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryBTreeSearchOp, 0, secondaryRTreeBulkLoad, 0);

        spec.addRoot(secondaryRTreeBulkLoad);
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
                secondaryRecDesc, storageManager, indexRegistryProvider, secondaryRTreeSplitProvider,
                secondaryTypeTraits, secondaryComparatorFactories, keyFields, dataflowHelperFactory, false, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryRTreeSearchOp, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, true,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, secondaryRTreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryRTreeSearchOp, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        File primaryBTree = new File(primaryBTreeFileName);
        primaryBTree.deleteOnExit();

        File secondary = new File(secondaryFileName);
        secondary.deleteOnExit();
    }
}