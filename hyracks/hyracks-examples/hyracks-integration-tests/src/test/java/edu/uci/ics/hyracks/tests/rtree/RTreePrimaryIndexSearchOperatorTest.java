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

public class RTreePrimaryIndexSearchOperatorTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    private IStorageManagerInterface storageManager = new TestStorageManagerInterface();
    private IIndexRegistryProvider<IIndex> indexRegistryProvider = new TestIndexRegistryProvider();
    private IIndexDataflowHelperFactory dataflowHelperFactory;

    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    private final static String sep = System.getProperty("file.separator");

    // field, type and key declarations for primary R-tree index
    private int primaryFieldCount = 5;
    private int primaryKeyFieldCount = 4;
    private ITypeTraits[] primaryTypeTraits = new ITypeTraits[primaryFieldCount];
    private IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];

    private RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE });

    private static String primaryRTreeName = "primary" + simpleDateFormat.format(new Date());
    private static String primaryFileName = System.getProperty("java.io.tmpdir") + sep + primaryRTreeName;

    private IFileSplitProvider primaryRTreeSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(primaryFileName))) });

    private IPrimitiveValueProviderFactory[] primaryValueProviderFactories;
    
    @Before
    public void setup() throws Exception {
        // field, type and key declarations for primary R-tree index
        primaryTypeTraits[0] = DoublePointable.TYPE_TRAITS;
        primaryTypeTraits[1] = DoublePointable.TYPE_TRAITS;
        primaryTypeTraits[2] = DoublePointable.TYPE_TRAITS;
        primaryTypeTraits[3] = DoublePointable.TYPE_TRAITS;
        primaryTypeTraits[4] = UTF8StringPointable.TYPE_TRAITS;
        primaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        primaryComparatorFactories[1] = primaryComparatorFactories[0];
        primaryComparatorFactories[2] = primaryComparatorFactories[0];
        primaryComparatorFactories[3] = primaryComparatorFactories[0];

        primaryValueProviderFactories = RTreeUtils
                .createPrimitiveValueProviderFactories(primaryComparatorFactories.length, DoublePointable.FACTORY);
        dataflowHelperFactory = new RTreeDataflowHelperFactory(primaryValueProviderFactories);
        
        createPrimaryIndex();
        loadPrimaryIndexTest();
    }

    public void createPrimaryIndex() throws Exception {
        JobSpecification spec = new JobSpecification();
        TreeIndexCreateOperatorDescriptor primaryCreateOp = new TreeIndexCreateOperatorDescriptor(spec, storageManager,
                indexRegistryProvider, primaryRTreeSplitProvider, primaryTypeTraits, primaryComparatorFactories,
                dataflowHelperFactory, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryCreateOp, NC1_ID);
        spec.addRoot(primaryCreateOp);
        runTest(spec);
    }
    
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
                storageManager, indexRegistryProvider, primaryRTreeSplitProvider, primaryTypeTraits, primaryComparatorFactories, fieldPermutation, 0.7f,
                dataflowHelperFactory, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryRTreeBulkLoad, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), objScanner, 0, primaryRTreeBulkLoad, 0);

        spec.addRoot(primaryRTreeBulkLoad);
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
                storageManager, indexRegistryProvider, primaryRTreeSplitProvider, primaryTypeTraits, primaryComparatorFactories, keyFields,
                dataflowHelperFactory, false, NoOpOperationCallbackProvider.INSTANCE);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryRTreeSearchOp, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, true,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryRTreeSearchOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), primaryRTreeSearchOp, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        File primary = new File(primaryFileName);
        primary.deleteOnExit();
    }
}