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
package org.apache.hyracks.examples.btree.client;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.examples.btree.helper.BTreeHelperStorageManager;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexDiskOrderScanOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

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

        @Option(name = "-frame-size", usage = "Hyracks frame size (default: 32768)", required = false)
        public int frameSize = 32768;
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

        JobSpecification spec = new JobSpecification(options.frameSize);

        String[] splitNCs = options.ncs.split(",");
        IStorageManager storageManager = BTreeHelperStorageManager.INSTANCE;

        // schema of tuples that we are retrieving from the primary index
        RecordDescriptor recDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                // we will use this as payload in secondary index
                IntegerSerializerDeserializer.INSTANCE,
                // we will use this ask key in secondary index
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer() });

        int primaryFieldCount = 4;
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[primaryFieldCount];
        primaryTypeTraits[0] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[2] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[3] = UTF8StringPointable.TYPE_TRAITS;

        // comparators for sort fields and BTree fields
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[2];
        comparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
        comparatorFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;

        // use a disk-order scan to read primary index
        IFileSplitProvider primarySplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.primaryBTreeName);
        IIndexDataflowHelperFactory primaryHelperFactory =
                new IndexDataflowHelperFactory(storageManager, primarySplitProvider);
        TreeIndexDiskOrderScanOperatorDescriptor btreeScanOp = new TreeIndexDiskOrderScanOperatorDescriptor(spec,
                recDesc, primaryHelperFactory, NoOpOperationCallbackFactory.INSTANCE);
        JobHelper.createPartitionConstraint(spec, btreeScanOp, splitNCs);

        // sort the tuples as preparation for bulk load into secondary index
        // fields to sort on
        int[] sortFields = { 1, 0 };
        ExternalSortOperatorDescriptor sorter =
                new ExternalSortOperatorDescriptor(spec, options.sbSize, sortFields, comparatorFactories, recDesc);
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
        IIndexDataflowHelperFactory secondaryHelperFactory =
                new IndexDataflowHelperFactory(storageManager, btreeSplitProvider);
        TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec, null,
                fieldPermutation, 0.7f, false, 1000L, true, secondaryHelperFactory);
        JobHelper.createPartitionConstraint(spec, btreeBulkLoad, splitNCs);
        NullSinkOperatorDescriptor nsOpDesc = new NullSinkOperatorDescriptor(spec);
        JobHelper.createPartitionConstraint(spec, nsOpDesc, splitNCs);

        // connect the ops
        spec.connect(new OneToOneConnectorDescriptor(spec), btreeScanOp, 0, sorter, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), btreeBulkLoad, 0, nsOpDesc, 0);
        spec.addRoot(nsOpDesc);

        return spec;
    }
}
