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

import static org.apache.hyracks.tests.am.btree.DataSetConstants.inputParserFactories;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.inputRecordDesc;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryFieldPermutation;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryKeyFieldCount;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.primaryRecDesc;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryFieldPermutationA;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryFieldPermutationB;
import static org.apache.hyracks.tests.am.btree.DataSetConstants.secondaryRecDesc;

import java.io.DataOutput;
import java.io.File;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
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
import org.apache.hyracks.storage.am.common.api.IIndexBuilderFactory;
import org.apache.hyracks.storage.am.common.api.IPageManagerFactory;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexInsertUpdateDeleteOperatorDescriptor;
import org.apache.hyracks.storage.am.common.freepage.AppendOnlyLinkedMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.test.support.TestStorageManager;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.tests.am.common.ITreeIndexOperatorTestHelper;
import org.apache.hyracks.tests.am.common.TreeOperatorTestHelper;
import org.apache.hyracks.tests.integration.AbstractIntegrationTest;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractBTreeOperatorTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    protected final IStorageManager storageManager = new TestStorageManager();
    protected final IPageManagerFactory pageManagerFactory = AppendOnlyLinkedMetadataPageManagerFactory.INSTANCE;

    // to be set by subclasses
    protected IFileSplitProvider primarySplitProvider;
    protected IIndexDataflowHelperFactory primaryHelperFactory;
    protected IFileSplitProvider secondarySplitProvider;
    protected IIndexDataflowHelperFactory secondaryHelperFactory;
    protected ITreeIndexOperatorTestHelper testHelper;

    protected ITreeIndexOperatorTestHelper createTestHelper() throws HyracksDataException {
        return new TreeOperatorTestHelper();
    }

    @Before
    public void setup() throws Exception {
        testHelper = createTestHelper();
        String primaryFileName = testHelper.getPrimaryIndexName();
        primarySplitProvider =
                new ConstantFileSplitProvider(new FileSplit[] { new ManagedFileSplit(NC1_ID, primaryFileName) });
        String secondaryFileName = testHelper.getSecondaryIndexName();
        primaryHelperFactory = new IndexDataflowHelperFactory(storageManager, primarySplitProvider);
        secondarySplitProvider =
                new ConstantFileSplitProvider(new FileSplit[] { new ManagedFileSplit(NC1_ID, secondaryFileName) });
        secondaryHelperFactory = new IndexDataflowHelperFactory(storageManager, secondarySplitProvider);
    }

    protected abstract IResourceFactory createPrimaryResourceFactory();

    protected abstract IResourceFactory createSecondaryResourceFactory();

    public void createPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IResourceFactory primaryResourceFactory = createPrimaryResourceFactory();
        IIndexBuilderFactory indexBuilderFactory =
                new IndexBuilderFactory(storageManager, primarySplitProvider, primaryResourceFactory, false);
        IndexCreateOperatorDescriptor primaryCreateOp = new IndexCreateOperatorDescriptor(spec, indexBuilderFactory);
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
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        int[] fieldPermutation = { 0, 1, 2, 4, 5, 7 };
        TreeIndexBulkLoadOperatorDescriptor primaryBtreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                primaryRecDesc, fieldPermutation, 0.7f, true, 1000L, true, primaryHelperFactory);
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
        IResourceFactory secondaryResourceFactory = createSecondaryResourceFactory();
        IIndexBuilderFactory indexBuilderFactory =
                new IndexBuilderFactory(storageManager, secondarySplitProvider, secondaryResourceFactory, false);
        IndexCreateOperatorDescriptor secondaryCreateOp = new IndexCreateOperatorDescriptor(spec, indexBuilderFactory);
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

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, keyProviderOp, NC1_ID);

        int[] lowKeyFields = null; // - infinity
        int[] highKeyFields = null; // + infinity

        // scan primary index
        BTreeSearchOperatorDescriptor primaryBtreeSearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                lowKeyFields, highKeyFields, true, true, primaryHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, null, null, false);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBtreeSearchOp, NC1_ID);

        // sort based on secondary keys
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec,
                1000, secondaryFieldPermutationA, new IBinaryComparatorFactory[] {
                        UTF8StringBinaryComparatorFactory.INSTANCE, UTF8StringBinaryComparatorFactory.INSTANCE },
                primaryRecDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID);

        // load secondary index
        int[] fieldPermutation = { 3, 0 };
        TreeIndexBulkLoadOperatorDescriptor secondaryBtreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                secondaryRecDesc, fieldPermutation, 0.7f, true, 1000L, true, secondaryHelperFactory);
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
                new TreeIndexInsertUpdateDeleteOperatorDescriptor(spec, ordersDesc, primaryFieldPermutation,
                        pipelineOperation, primaryHelperFactory, null, NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryBtreeInsertOp, NC1_ID);

        // first secondary index
        int[] fieldPermutationB = secondaryFieldPermutationB;
        TreeIndexInsertUpdateDeleteOperatorDescriptor secondaryInsertOp =
                new TreeIndexInsertUpdateDeleteOperatorDescriptor(spec, ordersDesc, fieldPermutationB,
                        pipelineOperation, secondaryHelperFactory, null, NoOpOperationCallbackFactory.INSTANCE);
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
        IndexDropOperatorDescriptor primaryDropOp = new IndexDropOperatorDescriptor(spec, primaryHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryDropOp, NC1_ID);
        spec.addRoot(primaryDropOp);
        runTest(spec);
    }

    protected void destroySecondaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        IndexDropOperatorDescriptor secondaryDropOp = new IndexDropOperatorDescriptor(spec, secondaryHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, secondaryDropOp, NC1_ID);
        spec.addRoot(secondaryDropOp);
        runTest(spec);
    }

    @After
    public abstract void cleanup() throws Exception;
}
