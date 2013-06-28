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
package edu.uci.ics.hyracks.examples.btree.client;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.IndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.examples.btree.helper.StorageManagerInterface;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDiskOrderScanOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

// This example will load a secondary index with <key, primary-index key> pairs
// We require an existing primary index built with PrimaryIndexBulkLoadExample

public class SecondaryIndexBulkLoadExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)")
        public int port = 1098;

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

        IHyracksClientConnection hcc = new HyracksConnection(options.host, options.port);

        JobSpecification job = createJob(options);

        long start = System.currentTimeMillis();
        JobId jobId = hcc.startJob(job);
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }

    private static JobSpecification createJob(Options options) {

        JobSpecification spec = new JobSpecification();

        String[] splitNCs = options.ncs.split(",");

        IIndexLifecycleManagerProvider lcManagerProvider = IndexLifecycleManagerProvider.INSTANCE;
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
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[primaryFieldCount];
        primaryTypeTraits[0] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[2] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[3] = UTF8StringPointable.TYPE_TRAITS;

        // comparators for sort fields and BTree fields
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);
        comparatorFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // use a disk-order scan to read primary index
        IFileSplitProvider primarySplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.primaryBTreeName);
        IIndexDataflowHelperFactory dataflowHelperFactory = new BTreeDataflowHelperFactory();
        TreeIndexDiskOrderScanOperatorDescriptor btreeScanOp = new TreeIndexDiskOrderScanOperatorDescriptor(spec,
                recDesc, storageManager, lcManagerProvider, primarySplitProvider, primaryTypeTraits,
                dataflowHelperFactory, NoOpOperationCallbackFactory.INSTANCE);
        JobHelper.createPartitionConstraint(spec, btreeScanOp, splitNCs);

        // sort the tuples as preparation for bulk load into secondary index
        // fields to sort on
        int[] sortFields = { 1, 0 };
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, options.sbSize, sortFields,
                comparatorFactories, recDesc);
        JobHelper.createPartitionConstraint(spec, sorter, splitNCs);

        // tuples to be put into B-Tree shall have 2 fields
        int secondaryFieldCount = 2;
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[secondaryFieldCount];
        secondaryTypeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        secondaryTypeTraits[1] = IntegerPointable.TYPE_TRAITS;

        // the B-Tree expects its keyfields to be at the front of its input
        // tuple
        int[] fieldPermutation = { 1, 0 };
        IFileSplitProvider btreeSplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.secondaryBTreeName);
        TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManager, lcManagerProvider, btreeSplitProvider, secondaryTypeTraits, comparatorFactories, null,
                fieldPermutation, 0.7f, false, 1000L, true, dataflowHelperFactory,
                NoOpOperationCallbackFactory.INSTANCE);
        JobHelper.createPartitionConstraint(spec, btreeBulkLoad, splitNCs);

        // connect the ops

        spec.connect(new OneToOneConnectorDescriptor(spec), btreeScanOp, 0, sorter, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);

        spec.addRoot(btreeBulkLoad);

        return spec;
    }
}