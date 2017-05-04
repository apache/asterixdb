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

package org.apache.hyracks.tests.am.btree;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexInsertUpdateDeleteOperatorDescriptor;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.freepage.LinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.file.TransientLocalResourceFactoryProvider;
import org.apache.hyracks.test.support.TestIndexLifecycleManagerProvider;
import org.apache.hyracks.test.support.TestStorageManager;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.tests.am.common.ITreeIndexOperatorTestHelper;
import org.apache.hyracks.tests.integration.AbstractIntegrationTest;
import org.junit.After;
import org.junit.Before;

import java.io.DataOutput;
import java.io.File;

import static org.apache.hyracks.tests.am.btree.DataSetConstants.inputParserFactories;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.inputRecordDesc;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryBloomFilterKeyFields;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryBtreeFields;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryComparatorFactories;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryFieldPermutation;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryFilterFields;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryKeyFieldCount;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryRecDesc;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryTypeTraits;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryBloomFilterKeyFields;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryBtreeFields;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryComparatorFactories;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryFieldPermutationA;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryFieldPermutationB;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryFilterFields;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryRecDesc;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryTypeTraits;

public abstract class AbstractBTreeOperatorTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    protected final IStorageManager storageManager = new TestStorageManager();
    protected final IIndexLifecycleManagerProvider lcManagerProvider = new TestIndexLifecycleManagerProvider();
    protected IIndexDataflowHelperFactory primaryDataflowHelperFactory;
    protected IIndexDataflowHelperFactory secondaryDataflowHelperFactory;

    // to be set by subclasses
    protected IFileSplitProvider primarySplitProvider;
    protected IPageManagerFactory pageManagerFactory = AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE;

    protected IFileSplitProvider secondarySplitProvider;

    protected ITreeIndexOperatorTestHelper testHelper;

    protected ITreeIndexOperatorTestHelper createTestHelper() throws HyracksDataException {
        return new BTreeOperatorTestHelper();
    }

    @Before
    public void setup() throws Exception {
        testHelper = createTestHelper();
        primaryDataflowHelperFactory = createDataFlowHelperFactory(primaryBtreeFields, primaryFilterFields);
        secondaryDataflowHelperFactory = createDataFlowHelperFactory(secondaryBtreeFields, secondaryFilterFields);
        String primaryFileName = testHelper.getPrimaryIndexName();
        primarySplitProvider =
                new ConstantFileSplitProvider(new FileSplit[] { new ManagedFileSplit(NC1_ID, primaryFileName) });
        String secondaryFileName = testHelper.getSecondaryIndexName();
        secondarySplitProvider =
                new ConstantFileSplitProvider(new FileSplit[] { new ManagedFileSplit(NC1_ID, secondaryFileName) });
    }

    protected abstract IIndexDataflowHelperFactory createDataFlowHelperFactory(int [] btreeFields, int[] filterFields);

    public void createPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        TransientLocalResourceFactoryProvider localResourceFactoryProvider =
                new TransientLocalResourceFactoryProvider();
        TreeIndexCreateOperatorDescriptor primaryCreateOp =
                new TreeIndexCreateOperatorDescriptor(spec, storageManager, lcManagerProvider, primarySplitProvider,
                        primaryTypeTraits, primaryComparatorFactories, primaryBloomFilterKeyFields,
                        primaryDataflowHelperFactory, localResourceFactoryProvider,
                        NoOpOperationCallbackFactory.INSTANCE, pageManagerFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryCreateOp, NC1_ID);
        spec.addRoot(primaryCreateOp);
        runTest(spec);
    }

    protected void loadPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new ManagedFileSplit(NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "orders-part1.tbl") };
        IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = inputRecordDesc;

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitProvider,
                new DelimitedDataTupleParserFactory(inputParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 1000, new int[] { 0 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        TreeIndexBulkLoadOperatorDescriptor primaryBtreeBulkLoad =
                new TreeIndexBulkLoadOperatorDescriptor(spec, primaryRecDesc, storageManager, lcManagerProvider,
                        primarySplitProvider, primaryTypeTraits, primaryComparatorFactories,
                        primaryBloomFilterKeyFields, primaryFieldPermutation, 0.7f, true, 1000L, true,
                        primaryDataflowHelperFactory, pageManagerFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBtreeBulkLoad, NC1_ID);

        NullSinkOperatorDescriptor nsOpDesc = new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, nsOpDesc, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, primaryBtreeBulkLoad, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryBtreeBulkLoad, 0, nsOpDesc, 0);

        spec.addRoot(nsOpDesc);
        runTest(spec);
    }

    public void createSecondaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        TransientLocalResourceFactoryProvider localResourceFactoryProvider =
                new TransientLocalResourceFactoryProvider();
        TreeIndexCreateOperatorDescriptor secondaryCreateOp =
                new TreeIndexCreateOperatorDescriptor(spec, storageManager, lcManagerProvider, secondarySplitProvider,
                        secondaryTypeTraits, secondaryComparatorFactories, secondaryBloomFilterKeyFields,
                        secondaryDataflowHelperFactory, localResourceFactoryProvider,
                        NoOpOperationCallbackFactory.INSTANCE, pageManagerFactory);
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

        RecordDescriptor keyRecDesc = secondaryRecDesc;

        ConstantTupleSourceOperatorDescriptor keyProviderOp =
                new ConstantTupleSourceOperatorDescriptor(spec, keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(),
                        tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);

        int[] lowKeyFields = null; // - infinity
        int[] highKeyFields = null; // + infinity

        // scan primary index
        BTreeSearchOperatorDescriptor primaryBtreeSearchOp =
                new BTreeSearchOperatorDescriptor(spec, primaryRecDesc, storageManager, lcManagerProvider,
                        primarySplitProvider, primaryTypeTraits, primaryComparatorFactories,
                        primaryBloomFilterKeyFields, lowKeyFields, highKeyFields, true, true,
                        secondaryDataflowHelperFactory, false, false, null, NoOpOperationCallbackFactory.INSTANCE, null,
                        null, new LinkedMetadataPageManagerFactory());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBtreeSearchOp, NC1_ID);

        // sort based on secondary keys
        ExternalSortOperatorDescriptor sorter =
                new ExternalSortOperatorDescriptor(spec, 1000, secondaryFieldPermutationA,
                        new IBinaryComparatorFactory[] {
                                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, primaryRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        // load secondary index
        int[] fieldPermutation = secondaryFieldPermutationA;
        TreeIndexBulkLoadOperatorDescriptor secondaryBtreeBulkLoad =
                new TreeIndexBulkLoadOperatorDescriptor(spec, secondaryRecDesc, storageManager, lcManagerProvider,
                        secondarySplitProvider, secondaryTypeTraits, secondaryComparatorFactories,
                        secondaryBloomFilterKeyFields, fieldPermutation, 0.7f, true, 1000L, true,
                        secondaryDataflowHelperFactory, pageManagerFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryBtreeBulkLoad, NC1_ID);

        NullSinkOperatorDescriptor nsOpDesc = new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, nsOpDesc, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryBtreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryBtreeSearchOp, 0, sorter, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, secondaryBtreeBulkLoad, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBtreeBulkLoad, 0, nsOpDesc, 0);

        spec.addRoot(nsOpDesc);
        runTest(spec);
    }

    protected void insertPipeline(boolean useUpsert) throws Exception {
        IndexOperation pipelineOperation = useUpsert ? IndexOperation.UPSERT : IndexOperation.INSERT;
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new ManagedFileSplit(NC1_ID,
                "data" + File.separator + "tpch0.002" + File.separator + "orders-part2.tbl") };
        IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = inputRecordDesc;

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitProvider,
                new DelimitedDataTupleParserFactory(inputParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        // insert into primary index
        TreeIndexInsertUpdateDeleteOperatorDescriptor primaryBtreeInsertOp =
                new TreeIndexInsertUpdateDeleteOperatorDescriptor(spec, ordersDesc, storageManager, lcManagerProvider,
                        primarySplitProvider, primaryTypeTraits, primaryComparatorFactories,
                        primaryBloomFilterKeyFields, primaryFieldPermutation, pipelineOperation,
                        primaryDataflowHelperFactory, null, NoOpOperationCallbackFactory.INSTANCE, pageManagerFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBtreeInsertOp, NC1_ID);

        // first secondary index
        int[] fieldPermutationB = secondaryFieldPermutationB;
        TreeIndexInsertUpdateDeleteOperatorDescriptor secondaryInsertOp =
                new TreeIndexInsertUpdateDeleteOperatorDescriptor(spec, ordersDesc, storageManager, lcManagerProvider,
                        secondarySplitProvider, secondaryTypeTraits, secondaryComparatorFactories,
                        secondaryBloomFilterKeyFields, fieldPermutationB, pipelineOperation,
                        secondaryDataflowHelperFactory, null, NoOpOperationCallbackFactory.INSTANCE,
                        pageManagerFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryInsertOp, NC1_ID);

        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, nullSink, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, primaryBtreeInsertOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primaryBtreeInsertOp, 0, secondaryInsertOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryInsertOp, 0, nullSink, 0);

        spec.addRoot(nullSink);
        runTest(spec);
    }

    protected void destroyPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IndexDropOperatorDescriptor primaryDropOp =
                new IndexDropOperatorDescriptor(spec, storageManager, lcManagerProvider, primarySplitProvider,
                        primaryDataflowHelperFactory, pageManagerFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryDropOp, NC1_ID);
        spec.addRoot(primaryDropOp);
        runTest(spec);
    }

    protected void destroySecondaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IndexDropOperatorDescriptor secondaryDropOp =
                new IndexDropOperatorDescriptor(spec, storageManager, lcManagerProvider, secondarySplitProvider,
                        secondaryDataflowHelperFactory, pageManagerFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryDropOp, NC1_ID);
        spec.addRoot(secondaryDropOp);
        runTest(spec);
    }

    @After
    public abstract void cleanup() throws Exception;
}
