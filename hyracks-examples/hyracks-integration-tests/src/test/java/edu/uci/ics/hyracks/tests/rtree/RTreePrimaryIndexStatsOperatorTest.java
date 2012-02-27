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
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
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
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexStatsOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.rtree.util.RTreeUtils;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestIndexRegistryProvider;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestStorageManagerInterface;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class RTreePrimaryIndexStatsOperatorTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    private IStorageManagerInterface storageManager = new TestStorageManagerInterface();
    private IIndexRegistryProvider<IIndex> indexRegistryProvider = new TestIndexRegistryProvider();
    private IIndexDataflowHelperFactory dataflowHelperFactory = new RTreeDataflowHelperFactory();

    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    private final static String sep = System.getProperty("file.separator");

    // field, type and key declarations for primary R-tree index
    private int primaryFieldCount = 5;
    private int primaryKeyFieldCount = 4;
    private ITypeTraits[] primaryTypeTraits = new ITypeTraits[primaryFieldCount];
    private IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];

    private RTreeTypeAwareTupleWriterFactory primaryTupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
            primaryTypeTraits);

    private RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE });

    private ITreeIndexFrameFactory primaryInteriorFrameFactory;
    private ITreeIndexFrameFactory primaryLeafFrameFactory;

    private static String primaryRTreeName = "primary" + simpleDateFormat.format(new Date());
    private static String primaryFileName = System.getProperty("java.io.tmpdir") + sep + primaryRTreeName;

    private IFileSplitProvider primaryRTreeSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(primaryFileName))) });

    @Before
    public void setup() throws Exception {
        // field, type and key declarations for primary R-tree index
        primaryTypeTraits[0] = DoublePointable.TYPE_TRAITS;
        primaryTypeTraits[1] = DoublePointable.TYPE_TRAITS;
        primaryTypeTraits[2] = DoublePointable.TYPE_TRAITS;
        primaryTypeTraits[3] = DoublePointable.TYPE_TRAITS;
        primaryTypeTraits[4] = DoublePointable.TYPE_TRAITS;
        primaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(DoublePointable.FACTORY);
        primaryComparatorFactories[1] = primaryComparatorFactories[0];
        primaryComparatorFactories[2] = primaryComparatorFactories[0];
        primaryComparatorFactories[3] = primaryComparatorFactories[0];

        IPrimitiveValueProviderFactory[] primaryValueProviderFactories = RTreeUtils
                .createPrimitiveValueProviderFactories(primaryComparatorFactories.length, DoublePointable.FACTORY);

        primaryInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(primaryTupleWriterFactory,
                primaryValueProviderFactories);
        primaryLeafFrameFactory = new RTreeNSMLeafFrameFactory(primaryTupleWriterFactory, primaryValueProviderFactories);

        loadPrimaryIndexTest();
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
                storageManager, indexRegistryProvider, primaryRTreeSplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, fieldPermutation, 0.7f,
                dataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryRTreeBulkLoad, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), objScanner, 0, primaryRTreeBulkLoad, 0);

        spec.addRoot(primaryRTreeBulkLoad);
        runTest(spec);
    }

    @Test
    public void showPrimaryIndexStats() throws Exception {
        JobSpecification spec = new JobSpecification();

        TreeIndexStatsOperatorDescriptor primaryStatsOp = new TreeIndexStatsOperatorDescriptor(spec, storageManager,
                indexRegistryProvider, primaryRTreeSplitProvider, primaryInteriorFrameFactory, primaryLeafFrameFactory,
                primaryTypeTraits, primaryComparatorFactories, dataflowHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryStatsOp, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, ",");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), primaryStatsOp, 0, printer, 0);
        spec.addRoot(printer);
        runTest(spec);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        File primary = new File(primaryFileName);
        primary.deleteOnExit();
    }
}