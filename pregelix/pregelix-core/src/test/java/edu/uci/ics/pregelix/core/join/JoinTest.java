/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.pregelix.core.join;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.join.InMemoryHashJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.TransientLocalResourceFactoryProvider;
import edu.uci.ics.pregelix.core.data.TypeTraits;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.pregelix.core.util.TestUtils;
import edu.uci.ics.pregelix.dataflow.VertexWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.IndexNestedLoopJoinOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.ProjectOperatorDescriptor;
import edu.uci.ics.pregelix.runtime.bootstrap.IndexLifeCycleManagerProvider;
import edu.uci.ics.pregelix.runtime.bootstrap.StorageManagerInterface;

public class JoinTest {
    private final static String ACTUAL_RESULT_DIR = "actual";
    private final static String EXPECT_RESULT_DIR = "expected";
    private final static String ACTUAL_RESULT_FILE = ACTUAL_RESULT_DIR + File.separator + "join.txt";
    private final static String EXPECTED_RESULT_FILE = EXPECT_RESULT_DIR + File.separator + "join.txt";
    private final static String JOB_NAME = "JOIN_TEST";
    private static final String HYRACKS_APP_NAME = "giraph";
    private static final String NC1_ID = "nc1";
    private static final String NC2_ID = "nc2";

    private static final String PATH_TO_CLUSTER_STORE = "src/test/resources/cluster/data.properties";
    private static final String PATH_TO_CLUSTER_PROPERTIES = "src/test/resources/cluster/cluster.properties";

    private static final float DEFAULT_BTREE_FILL_FACTOR = 1.00f;
    private IIndexLifecycleManagerProvider lcManagerProvider = IndexLifeCycleManagerProvider.INSTANCE;
    private IStorageManagerInterface storageManagerInterface = StorageManagerInterface.INSTANCE;

    private IBinaryHashFunctionFactory stringHashFactory = new PointableBinaryHashFunctionFactory(
            UTF8StringPointable.FACTORY);
    private IBinaryComparatorFactory stringComparatorFactory = new PointableBinaryComparatorFactory(
            UTF8StringPointable.FACTORY);
    private INormalizedKeyComputerFactory nmkFactory = new UTF8StringNormalizedKeyComputerFactory();

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    @Test
    public void customerOrderCIDJoinMulti() throws Exception {
        ClusterConfig.setStorePath(PATH_TO_CLUSTER_STORE);
        ClusterConfig.setClusterPropertiesPath(PATH_TO_CLUSTER_PROPERTIES);
        cleanupStores();
        PregelixHyracksIntegrationUtil.init();

        FileUtils.forceMkdir(new File(EXPECT_RESULT_DIR));
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(EXPECT_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        runCreate();
        runBulkLoad();
        runHashJoin();
        runIndexJoin();
        TestUtils.compareWithResult(new File(EXPECTED_RESULT_FILE), new File(ACTUAL_RESULT_FILE));

        FileUtils.cleanDirectory(new File(EXPECT_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        runLeftOuterHashJoin();
        runIndexRightOuterJoin();
        TestUtils.compareWithResult(new File(EXPECTED_RESULT_FILE), new File(ACTUAL_RESULT_FILE));

        PregelixHyracksIntegrationUtil.deinit();
    }

    private void runHashJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 1 },
                new int[] { 0 }, new IBinaryHashFunctionFactory[] { stringHashFactory },
                new IBinaryComparatorFactory[] { stringComparatorFactory }, custOrderJoinDesc, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        int[] sortFields = new int[2];
        sortFields[0] = 1;
        sortFields[1] = 0;
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = stringComparatorFactory;
        comparatorFactories[1] = stringComparatorFactory;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 1024, sortFields,
                comparatorFactories, custOrderJoinDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID, NC2_ID);

        FileSplit resultFile = new FileSplit(NC1_ID, new FileReference(new File(EXPECTED_RESULT_FILE)));
        FileSplit[] results = new FileSplit[1];
        results[0] = resultFile;
        IFileSplitProvider resultFileSplitProvider = new ConstantFileSplitProvider(results);
        VertexWriteOperatorDescriptor writer = new VertexWriteOperatorDescriptor(spec, null, resultFileSplitProvider,
                null, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, writer, new String[] { NC1_ID });
        PartitionConstraintHelper.addPartitionCountConstraint(spec, writer, 1);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { stringHashFactory }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { stringHashFactory }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, sorter, 0);
        IConnectorDescriptor joinWriterConn = new MToNPartitioningMergingConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { stringHashFactory }), sortFields, comparatorFactories,
                nmkFactory);
        spec.connect(joinWriterConn, sorter, 0, writer, 0);

        spec.addRoot(writer);
        runTest(spec);
    }

    private void runCreate() throws Exception {
        JobSpecification spec = new JobSpecification();
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = stringComparatorFactory;
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(JOB_NAME, JOB_NAME);
        ITypeTraits[] typeTraits = new ITypeTraits[custDesc.getFields().length];
        for (int i = 0; i < typeTraits.length; i++)
            typeTraits[i] = new TypeTraits(false);
        TreeIndexCreateOperatorDescriptor writer = new TreeIndexCreateOperatorDescriptor(spec, storageManagerInterface,
                lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories, null,
                new BTreeDataflowHelperFactory(), new TransientLocalResourceFactoryProvider(),
                NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, writer, NC1_ID, NC2_ID);
        spec.addRoot(writer);
        runTest(spec);
    }

    private void runBulkLoad() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        int[] sortFields = new int[1];
        sortFields[0] = 0;
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = stringComparatorFactory;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 1024, sortFields,
                comparatorFactories, custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID, NC2_ID);

        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(JOB_NAME, JOB_NAME);
        int[] fieldPermutation = new int[custDesc.getFields().length];
        for (int i = 0; i < fieldPermutation.length; i++)
            fieldPermutation[i] = i;
        ITypeTraits[] typeTraits = new ITypeTraits[custDesc.getFields().length];
        for (int i = 0; i < typeTraits.length; i++)
            typeTraits[i] = new TypeTraits(false);
        TreeIndexBulkLoadOperatorDescriptor writer = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories, null,
                fieldPermutation, DEFAULT_BTREE_FILL_FACTOR, false, 0, false, new BTreeDataflowHelperFactory(),
                NoOpOperationCallbackFactory.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, writer, NC1_ID, NC2_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), custScanner, 0, sorter, 0);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(
                sortFields, new IBinaryHashFunctionFactory[] { stringHashFactory }), sortFields, comparatorFactories,
                nmkFactory), sorter, 0, writer, 0);

        spec.addRoot(writer);
        runTest(spec);
    }

    private void runIndexJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        /** sort operator */
        int[] sortFields = new int[2];
        sortFields[0] = 1;
        sortFields[1] = 0;
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = stringComparatorFactory;
        comparatorFactories[1] = stringComparatorFactory;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 1024, sortFields,
                comparatorFactories, ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID, NC2_ID);

        /** index join operator */
        int[] keyFields = new int[1];
        keyFields[0] = 1;
        IBinaryComparatorFactory[] keyComparatorFactories = new IBinaryComparatorFactory[1];
        keyComparatorFactories[0] = stringComparatorFactory;
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(JOB_NAME, JOB_NAME);
        ITypeTraits[] typeTraits = new ITypeTraits[custDesc.getFields().length];
        for (int i = 0; i < typeTraits.length; i++)
            typeTraits[i] = new TypeTraits(false);
        IndexNestedLoopJoinOperatorDescriptor join = new IndexNestedLoopJoinOperatorDescriptor(spec, custOrderJoinDesc,
                storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits, keyComparatorFactories,
                true, keyFields, keyFields, true, true, new BTreeDataflowHelperFactory());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        /** results (already in sorted order) */
        FileSplit resultFile = new FileSplit(NC1_ID, new FileReference(new File(ACTUAL_RESULT_FILE)));
        FileSplit[] results = new FileSplit[1];
        results[0] = resultFile;
        IFileSplitProvider resultFileSplitProvider = new ConstantFileSplitProvider(results);
        VertexWriteOperatorDescriptor writer = new VertexWriteOperatorDescriptor(spec, null, resultFileSplitProvider,
                null, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, writer, new String[] { NC1_ID });
        PartitionConstraintHelper.addPartitionCountConstraint(spec, writer, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(
                keyFields, new IBinaryHashFunctionFactory[] { stringHashFactory }), sortFields, comparatorFactories,
                nmkFactory), sorter, 0, join, 0);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(
                keyFields, new IBinaryHashFunctionFactory[] { stringHashFactory }), sortFields, comparatorFactories,
                nmkFactory), join, 0, writer, 0);

        spec.addRoot(writer);
        runTest(spec);
    }

    private void runLeftOuterHashJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[] { JoinTestNullWriterFactory.INSTANCE,
                JoinTestNullWriterFactory.INSTANCE, JoinTestNullWriterFactory.INSTANCE,
                JoinTestNullWriterFactory.INSTANCE, JoinTestNullWriterFactory.INSTANCE,
                JoinTestNullWriterFactory.INSTANCE, JoinTestNullWriterFactory.INSTANCE,
                JoinTestNullWriterFactory.INSTANCE, JoinTestNullWriterFactory.INSTANCE };

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 0 },
                new int[] { 1 }, new IBinaryHashFunctionFactory[] { stringHashFactory },
                new IBinaryComparatorFactory[] { stringComparatorFactory }, custOrderJoinDesc, true,
                nullWriterFactories, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        int[] projectFields = new int[] { 8, 9, 10, 11, 12, 13, 14, 15, 16, 0, 1, 2, 3, 4, 5, 6, 7 };
        ProjectOperatorDescriptor project = new ProjectOperatorDescriptor(spec, custOrderJoinDesc, projectFields);

        int[] sortFields = new int[2];
        sortFields[0] = 9;
        sortFields[1] = 0;
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = stringComparatorFactory;
        comparatorFactories[1] = stringComparatorFactory;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 1024, sortFields,
                comparatorFactories, custOrderJoinDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID, NC2_ID);

        FileSplit resultFile = new FileSplit(NC1_ID, new FileReference(new File(EXPECTED_RESULT_FILE)));
        FileSplit[] results = new FileSplit[1];
        results[0] = resultFile;
        IFileSplitProvider resultFileSplitProvider = new ConstantFileSplitProvider(results);
        VertexWriteOperatorDescriptor writer = new VertexWriteOperatorDescriptor(spec, null, resultFileSplitProvider,
                null, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, writer, new String[] { NC1_ID });
        PartitionConstraintHelper.addPartitionCountConstraint(spec, writer, 1);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { stringHashFactory }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { stringHashFactory }));
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, project, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), project, 0, sorter, 0);
        IConnectorDescriptor joinWriterConn = new MToNPartitioningMergingConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 9 },
                        new IBinaryHashFunctionFactory[] { stringHashFactory }), sortFields, comparatorFactories,
                nmkFactory);
        spec.connect(joinWriterConn, sorter, 0, writer, 0);

        spec.addRoot(writer);
        runTest(spec);
    }

    private void runIndexRightOuterJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[] { JoinTestNullWriterFactory.INSTANCE,
                JoinTestNullWriterFactory.INSTANCE, JoinTestNullWriterFactory.INSTANCE,
                JoinTestNullWriterFactory.INSTANCE, JoinTestNullWriterFactory.INSTANCE,
                JoinTestNullWriterFactory.INSTANCE, JoinTestNullWriterFactory.INSTANCE,
                JoinTestNullWriterFactory.INSTANCE, JoinTestNullWriterFactory.INSTANCE };

        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        /** sort operator */
        int[] sortFields = new int[2];
        sortFields[0] = 1;
        sortFields[1] = 0;
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = stringComparatorFactory;
        comparatorFactories[1] = stringComparatorFactory;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 1024, sortFields,
                comparatorFactories, ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID, NC2_ID);

        /** index join operator */
        int[] keyFields = new int[1];
        keyFields[0] = 1;
        IBinaryComparatorFactory[] keyComparatorFactories = new IBinaryComparatorFactory[1];
        keyComparatorFactories[0] = stringComparatorFactory;
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(JOB_NAME, JOB_NAME);
        ITypeTraits[] typeTraits = new ITypeTraits[custDesc.getFields().length];
        for (int i = 0; i < typeTraits.length; i++)
            typeTraits[i] = new TypeTraits(false);
        ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(new TypeAwareTupleWriterFactory(
                typeTraits));
        ITreeIndexFrameFactory leafFrameFactory = new BTreeNSMLeafFrameFactory(new TypeAwareTupleWriterFactory(
                typeTraits));
        IndexNestedLoopJoinOperatorDescriptor join = new IndexNestedLoopJoinOperatorDescriptor(spec, custOrderJoinDesc,
                storageManagerInterface, lcManagerProvider, fileSplitProvider, interiorFrameFactory, leafFrameFactory,
                typeTraits, keyComparatorFactories, true, keyFields, keyFields, true, true,
                new BTreeDataflowHelperFactory(), true, nullWriterFactories);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        /** results (already in sorted order) */
        FileSplit resultFile = new FileSplit(NC1_ID, new FileReference(new File(ACTUAL_RESULT_FILE)));
        FileSplit[] results = new FileSplit[1];
        results[0] = resultFile;
        IFileSplitProvider resultFileSplitProvider = new ConstantFileSplitProvider(results);
        VertexWriteOperatorDescriptor writer = new VertexWriteOperatorDescriptor(spec, null, resultFileSplitProvider,
                null, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, writer, new String[] { NC1_ID });
        PartitionConstraintHelper.addPartitionCountConstraint(spec, writer, 1);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(
                keyFields, new IBinaryHashFunctionFactory[] { new PointableBinaryHashFunctionFactory(
                        UTF8StringPointable.FACTORY) }), sortFields, comparatorFactories, nmkFactory), sorter, 0, join,
                0);

        IBinaryComparatorFactory[] mergeComparatorFactories = new IBinaryComparatorFactory[2];
        mergeComparatorFactories[0] = new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY);
        mergeComparatorFactories[1] = new PointableBinaryComparatorFactory(UTF8StringPointable.FACTORY);
        int[] mergeFields = new int[] { 9, 0 };
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(
                new int[] { 9 }, new IBinaryHashFunctionFactory[] { new PointableBinaryHashFunctionFactory(
                        UTF8StringPointable.FACTORY) }), mergeFields, comparatorFactories, nmkFactory), join, 0,
                writer, 0);

        spec.addRoot(writer);
        runTest(spec);
    }

    private void runTest(JobSpecification spec) throws Exception {
        PregelixHyracksIntegrationUtil.runJob(spec, HYRACKS_APP_NAME);
    }
}
