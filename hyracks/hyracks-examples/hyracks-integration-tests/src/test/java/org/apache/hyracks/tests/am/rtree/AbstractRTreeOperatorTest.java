/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.tests.am.rtree;

import java.io.DataOutput;
import java.io.File;

import org.junit.After;
import org.junit.Before;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.TreeIndexException;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexInsertUpdateDeleteOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;
import org.apache.hyracks.storage.common.IStorageManagerInterface;
import org.apache.hyracks.storage.common.file.TransientLocalResourceFactoryProvider;
import org.apache.hyracks.test.support.TestIndexLifecycleManagerProvider;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestStorageManagerInterface;
import org.apache.hyracks.tests.am.common.ITreeIndexOperatorTestHelper;
import org.apache.hyracks.tests.integration.AbstractIntegrationTest;

public abstract class AbstractRTreeOperatorTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    protected enum RTreeType {
        LSMRTREE,
        LSMRTREE_WITH_ANTIMATTER,
        RTREE
    };

    protected RTreeType rTreeType;

    protected final IStorageManagerInterface storageManager = new TestStorageManagerInterface();
    protected final IIndexLifecycleManagerProvider lcManagerProvider = new TestIndexLifecycleManagerProvider();
    protected IIndexDataflowHelperFactory rtreeDataflowHelperFactory;
    protected IIndexDataflowHelperFactory btreeDataflowHelperFactory = new BTreeDataflowHelperFactory(true);

    // field, type and key declarations for primary index
    protected final int primaryFieldCount = 10;
    protected final ITypeTraits[] primaryTypeTraits = new ITypeTraits[primaryFieldCount];
    protected final int primaryKeyFieldCount = 1;
    protected final IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];

    protected final RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE });

    // to be set by subclasses
    protected String primaryFileName;
    protected IFileSplitProvider primarySplitProvider;

    // field, type and key declarations for secondary indexes
    protected final int secondaryFieldCount = 5;
    protected final ITypeTraits[] secondaryTypeTraits = new ITypeTraits[secondaryFieldCount];
    protected final int secondaryKeyFieldCount = 4;
    protected final IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[secondaryKeyFieldCount];

    protected final RecordDescriptor secondaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer() });

    // This is only used for the LSMRTree. We need a comparator Factories for
    // the BTree component of the LSMRTree.
    protected int btreeKeyFieldCount = 5;
    protected IBinaryComparatorFactory[] btreeComparatorFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];

    protected String secondaryFileName;
    protected IFileSplitProvider secondarySplitProvider;

    protected ITreeIndexOperatorTestHelper testHelper;

    protected ITreeIndexOperatorTestHelper createTestHelper() throws HyracksException {
        return new RTreeOperatorTestHelper();
    }

    @Before
    public void setup() throws Exception {
        testHelper = createTestHelper();

        primaryFileName = testHelper.getPrimaryIndexName();
        primarySplitProvider = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID, new FileReference(
                new File(primaryFileName))) });
        secondaryFileName = testHelper.getSecondaryIndexName();
        secondarySplitProvider = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                new FileReference(new File(secondaryFileName))) });

        // field, type and key declarations for primary index
        primaryTypeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[2] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[3] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[4] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[5] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[6] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[7] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[8] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[9] = UTF8StringPointable.TYPE_TRAITS;
        primaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);

        // field, type and key declarations for secondary indexes
        secondaryTypeTraits[0] = DoublePointable.TYPE_TRAITS;
        secondaryTypeTraits[1] = DoublePointable.TYPE_TRAITS;
        secondaryTypeTraits[2] = DoublePointable.TYPE_TRAITS;
        secondaryTypeTraits[3] = DoublePointable.TYPE_TRAITS;
        secondaryTypeTraits[4] = UTF8StringPointable.TYPE_TRAITS;
        secondaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        secondaryComparatorFactories[1] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        secondaryComparatorFactories[2] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        secondaryComparatorFactories[3] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);

        // This only used for LSMRTree
        int[] btreeFields = null;
        if (rTreeType == RTreeType.LSMRTREE) {
            btreeKeyFieldCount = 1;
            btreeComparatorFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
            btreeComparatorFactories[0] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);
            btreeFields = new int[1];
            btreeFields[0] = 4;

        } else {
            btreeComparatorFactories[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
            btreeComparatorFactories[1] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
            btreeComparatorFactories[2] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
            btreeComparatorFactories[3] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
            btreeComparatorFactories[4] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);
        }

        IPrimitiveValueProviderFactory[] secondaryValueProviderFactories = RTreeUtils
                .createPrimitiveValueProviderFactories(secondaryComparatorFactories.length, DoublePointable.FACTORY);

        rtreeDataflowHelperFactory = createDataFlowHelperFactory(secondaryValueProviderFactories,
                RTreePolicyType.RSTARTREE, btreeComparatorFactories,
                LSMRTreeUtils.proposeBestLinearizer(secondaryTypeTraits, secondaryComparatorFactories.length),
                btreeFields);

    }

    protected abstract IIndexDataflowHelperFactory createDataFlowHelperFactory(
            IPrimitiveValueProviderFactory[] secondaryValueProviderFactories, RTreePolicyType rtreePolicyType,
            IBinaryComparatorFactory[] btreeComparatorFactories, ILinearizeComparatorFactory linearizerCmpFactory,
            int[] btreeFields) throws TreeIndexException;

    protected void createPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        TransientLocalResourceFactoryProvider localResourceFactoryProvider = new TransientLocalResourceFactoryProvider();
        TreeIndexCreateOperatorDescriptor primaryCreateOp = new TreeIndexCreateOperatorDescriptor(spec, storageManager,
                lcManagerProvider, primarySplitProvider, primaryTypeTraits, primaryComparatorFactories, null,
                btreeDataflowHelperFactory, localResourceFactoryProvider, NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryCreateOp, NC1_ID);
        spec.addRoot(primaryCreateOp);
        runTest(spec);
    }

    protected void loadPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/orders-with-locations-part1.txt"))) };
        IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), DoubleSerializerDeserializer.INSTANCE,
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
        TreeIndexBulkLoadOperatorDescriptor primaryBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                primaryRecDesc, storageManager, lcManagerProvider, primarySplitProvider, primaryTypeTraits,
                primaryComparatorFactories, null, fieldPermutation, 0.7f, false, 1000L, true,
                btreeDataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBulkLoad, NC1_ID);

        NullSinkOperatorDescriptor nsOpDesc = new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, nsOpDesc, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, primaryBulkLoad, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primaryBulkLoad, 0, nsOpDesc, 0);

        spec.addRoot(nsOpDesc);
        runTest(spec);
    }

    protected void createSecondaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        TransientLocalResourceFactoryProvider localResourceFactoryProvider = new TransientLocalResourceFactoryProvider();
        TreeIndexCreateOperatorDescriptor secondaryCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                storageManager, lcManagerProvider, secondarySplitProvider, secondaryTypeTraits,
                secondaryComparatorFactories, null, rtreeDataflowHelperFactory, localResourceFactoryProvider,
                NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryCreateOp, NC1_ID);
        spec.addRoot(secondaryCreateOp);
        runTest(spec);
    }

    protected void loadSecondaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();

        // build dummy tuple containing nothing
        ArrayTupleBuilder tb = new ArrayTupleBuilder(primaryKeyFieldCount * 2);
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        new UTF8StringSerializerDeserializer().serialize("0", dos);
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer() };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);

        int[] lowKeyFields = null; // - infinity
        int[] highKeyFields = null; // + infinity

        // scan primary index
        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                storageManager, lcManagerProvider, primarySplitProvider, primaryTypeTraits, primaryComparatorFactories,
                null, lowKeyFields, highKeyFields, true, true, btreeDataflowHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, null, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primarySearchOp, NC1_ID);

        // load secondary index
        int[] fieldPermutation = { 6, 7, 8, 9, 0 };
        TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                secondaryRecDesc, storageManager, lcManagerProvider, secondarySplitProvider, secondaryTypeTraits,
                secondaryComparatorFactories, null, fieldPermutation, 0.7f, false, 1000L, true,
                rtreeDataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryBulkLoad, NC1_ID);

        NullSinkOperatorDescriptor nsOpDesc = new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, nsOpDesc, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primarySearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primarySearchOp, 0, secondaryBulkLoad, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoad, 0, nsOpDesc, 0);

        spec.addRoot(nsOpDesc);
        runTest(spec);
    }

    protected void insertPipeline() throws Exception {

        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/orders-with-locations-part2.txt"))) };
        IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), DoubleSerializerDeserializer.INSTANCE,
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

        // insert into primary index
        int[] primaryFieldPermutation = { 0, 1, 2, 4, 5, 7, 9, 10, 11, 12 };
        TreeIndexInsertUpdateDeleteOperatorDescriptor primaryInsertOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, ordersDesc, storageManager, lcManagerProvider, primarySplitProvider, primaryTypeTraits,
                primaryComparatorFactories, null, primaryFieldPermutation, IndexOperation.INSERT,
                btreeDataflowHelperFactory, null, NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryInsertOp, NC1_ID);

        // secondary index
        int[] secondaryFieldPermutation = { 9, 10, 11, 12, 0 };
        TreeIndexInsertUpdateDeleteOperatorDescriptor secondaryInsertOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, ordersDesc, storageManager, lcManagerProvider, secondarySplitProvider, secondaryTypeTraits,
                secondaryComparatorFactories, null, secondaryFieldPermutation, IndexOperation.INSERT,
                rtreeDataflowHelperFactory, null, NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryInsertOp, NC1_ID);

        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, nullSink, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, primaryInsertOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primaryInsertOp, 0, secondaryInsertOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryInsertOp, 0, nullSink, 0);

        spec.addRoot(nullSink);
        runTest(spec);
    }

    protected void destroyPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IndexDropOperatorDescriptor primaryDropOp = new IndexDropOperatorDescriptor(spec, storageManager,
                lcManagerProvider, primarySplitProvider, btreeDataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryDropOp, NC1_ID);
        spec.addRoot(primaryDropOp);
        runTest(spec);
    }

    protected void destroySecondaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IndexDropOperatorDescriptor secondaryDropOp = new IndexDropOperatorDescriptor(spec, storageManager,
                lcManagerProvider, secondarySplitProvider, rtreeDataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryDropOp, NC1_ID);
        spec.addRoot(secondaryDropOp);
        runTest(spec);
    }

    @After
    public abstract void cleanup() throws Exception;
}