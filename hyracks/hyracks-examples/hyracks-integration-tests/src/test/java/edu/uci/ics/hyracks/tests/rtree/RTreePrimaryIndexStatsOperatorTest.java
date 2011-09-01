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
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.DoubleBinaryComparatorFactory;
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
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexStatsOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreeNSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.rtree.impls.DoublePrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.rtree.tuples.RTreeTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestStorageManagerComponentHolder;
import edu.uci.ics.hyracks.test.support.TestStorageManagerInterface;
import edu.uci.ics.hyracks.test.support.TestTreeIndexRegistryProvider;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public class RTreePrimaryIndexStatsOperatorTest extends AbstractIntegrationTest {
    static {
        TestStorageManagerComponentHolder.init(8192, 20, 20);
    }

    private IStorageManagerInterface storageManager = new TestStorageManagerInterface();
    private IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider = new TestTreeIndexRegistryProvider();
    private ITreeIndexOpHelperFactory opHelperFactory = new RTreeOpHelperFactory();

    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("ddMMyy-hhmmssSS");
    private final static String sep = System.getProperty("file.separator");

    // field, type and key declarations for primary R-tree index
    private int primaryFieldCount = 5;
    private int primaryKeyFieldCount = 4;
    private ITypeTrait[] primaryTypeTraits = new ITypeTrait[primaryFieldCount];
    private IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[primaryKeyFieldCount];
    private IPrimitiveValueProviderFactory[] primaryValueProviderFactories = new IPrimitiveValueProviderFactory[primaryKeyFieldCount];

    private RTreeTypeAwareTupleWriterFactory primaryTupleWriterFactory = new RTreeTypeAwareTupleWriterFactory(
            primaryTypeTraits);

    private RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE });

    private ITreeIndexFrameFactory primaryInteriorFrameFactory = new RTreeNSMInteriorFrameFactory(
            primaryTupleWriterFactory, primaryKeyFieldCount);
    private ITreeIndexFrameFactory primaryLeafFrameFactory = new RTreeNSMLeafFrameFactory(primaryTupleWriterFactory,
            primaryKeyFieldCount);

    private static String primaryRTreeName = "primary" + simpleDateFormat.format(new Date());
    private static String primaryFileName = System.getProperty("java.io.tmpdir") + sep + primaryRTreeName;

    private IFileSplitProvider primaryRTreeSplitProvider = new ConstantFileSplitProvider(
            new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(primaryFileName))) });

    @Before
    public void setup() throws Exception {
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
        primaryValueProviderFactories[0] = DoublePrimitiveValueProviderFactory.INSTANCE;
        primaryValueProviderFactories[1] = primaryValueProviderFactories[0];
        primaryValueProviderFactories[2] = primaryValueProviderFactories[0];
        primaryValueProviderFactories[3] = primaryValueProviderFactories[0];

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
                storageManager, treeIndexRegistryProvider, primaryRTreeSplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, primaryValueProviderFactories,
                fieldPermutation, 0.7f, opHelperFactory);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, primaryRTreeBulkLoad, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), objScanner, 0, primaryRTreeBulkLoad, 0);

        spec.addRoot(primaryRTreeBulkLoad);
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

    @AfterClass
    public static void cleanup() throws Exception {
        File primary = new File(primaryFileName);
        primary.deleteOnExit();
    }
}