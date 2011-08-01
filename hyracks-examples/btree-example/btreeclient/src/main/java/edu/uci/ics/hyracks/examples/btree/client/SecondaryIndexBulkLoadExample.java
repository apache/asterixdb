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
package edu.uci.ics.hyracks.examples.btree.client;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.BTreeRegistryProvider;
import edu.uci.ics.hyracks.examples.btree.helper.StorageManagerInterface;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDiskOrderScanOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

// This example will load a secondary index with <key, primary-index key> pairs
// We require an existing primary index built with PrimaryIndexBulkLoadExample

public class SecondaryIndexBulkLoadExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;

        @Option(name = "-primary-btreename", usage = "Name of primary-index B-Tree to load from", required = true)
        public String primaryBTreeName;

        @Option(name = "-secondary-btreename", usage = "B-Tree file name for secondary index to be built", required = true)
        public String secondaryBTreeName;

        @Option(name = "-sortbuffer-size", usage = "Sort buffer size in frames (default: 32768)", required = false)
        public int sbSize = 32768;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job = createJob(options);

        long start = System.currentTimeMillis();
        JobId jobId = hcc.createJob(options.app, job);
        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }

    private static JobSpecification createJob(Options options) {

        JobSpecification spec = new JobSpecification();

        String[] splitNCs = options.ncs.split(",");

        IBTreeRegistryProvider btreeRegistryProvider = BTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = StorageManagerInterface.INSTANCE;

        // schema of tuples that we are retrieving from the primary index
        RecordDescriptor recDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, // we will use this as
                                                        // payload in secondary
                                                        // index
                UTF8StringSerializerDeserializer.INSTANCE, // we will use this
                                                           // ask key in
                                                           // secondary index
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        int primaryFieldCount = 4;
        ITypeTrait[] primaryTypeTraits = new ITypeTrait[primaryFieldCount];
        primaryTypeTraits[0] = new TypeTrait(4);
        primaryTypeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryTypeTraits[2] = new TypeTrait(4);
        primaryTypeTraits[3] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);

        // create factories and providers for primary B-Tree
        TypeAwareTupleWriterFactory primaryTupleWriterFactory = new TypeAwareTupleWriterFactory(primaryTypeTraits);
        IBTreeInteriorFrameFactory primaryInteriorFrameFactory = new NSMInteriorFrameFactory(primaryTupleWriterFactory);
        IBTreeLeafFrameFactory primaryLeafFrameFactory = new NSMLeafFrameFactory(primaryTupleWriterFactory);

        // use a disk-order scan to read primary index
        IFileSplitProvider primarySplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.primaryBTreeName);
        BTreeDiskOrderScanOperatorDescriptor btreeScanOp = new BTreeDiskOrderScanOperatorDescriptor(spec, recDesc,
                storageManager, btreeRegistryProvider, primarySplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits);
        JobHelper.createPartitionConstraint(spec, btreeScanOp, splitNCs);

        // sort the tuples as preparation for bulk load into secondary index
        // fields to sort on
        int[] sortFields = { 1, 0 };
        // comparators for sort fields
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
        comparatorFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, options.sbSize, sortFields,
                comparatorFactories, recDesc);
        JobHelper.createPartitionConstraint(spec, sorter, splitNCs);

        // tuples to be put into B-Tree shall have 2 fields
        int secondaryFieldCount = 2;
        ITypeTrait[] secondaryTypeTraits = new ITypeTrait[secondaryFieldCount];
        secondaryTypeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        secondaryTypeTraits[1] = new TypeTrait(4);

        // create factories and providers for secondary B-Tree
        TypeAwareTupleWriterFactory secondaryTupleWriterFactory = new TypeAwareTupleWriterFactory(secondaryTypeTraits);
        IBTreeInteriorFrameFactory secondaryInteriorFrameFactory = new NSMInteriorFrameFactory(
                secondaryTupleWriterFactory);
        IBTreeLeafFrameFactory secondaryLeafFrameFactory = new NSMLeafFrameFactory(secondaryTupleWriterFactory);

        // the B-Tree expects its keyfields to be at the front of its input
        // tuple
        int[] fieldPermutation = { 1, 0 };
        IFileSplitProvider btreeSplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.secondaryBTreeName);
        BTreeBulkLoadOperatorDescriptor btreeBulkLoad = new BTreeBulkLoadOperatorDescriptor(spec, storageManager,
                btreeRegistryProvider, btreeSplitProvider, secondaryInteriorFrameFactory, secondaryLeafFrameFactory,
                secondaryTypeTraits, comparatorFactories, fieldPermutation, 0.7f);
        JobHelper.createPartitionConstraint(spec, btreeBulkLoad, splitNCs);

        // connect the ops

        spec.connect(new OneToOneConnectorDescriptor(spec), btreeScanOp, 0, sorter, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);

        spec.addRoot(btreeBulkLoad);

        return spec;
    }
}