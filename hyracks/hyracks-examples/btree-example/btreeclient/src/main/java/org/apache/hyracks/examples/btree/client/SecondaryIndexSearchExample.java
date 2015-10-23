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

import java.io.DataOutput;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import org.apache.hyracks.examples.btree.helper.IndexLifecycleManagerProvider;
import org.apache.hyracks.examples.btree.helper.StorageManagerInterface;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.common.IStorageManagerInterface;

// This example will perform range search on the secondary index
// and then retrieve the corresponding source records from the primary index

public class SecondaryIndexSearchExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)")
        public int port = 1098;

        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;

        @Option(name = "-primary-btreename", usage = "Primary B-Tree file name", required = true)
        public String primaryBTreeName;

        @Option(name = "-secondary-btreename", usage = "Secondary B-Tree file name to search", required = true)
        public String secondaryBTreeName;

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

    private static JobSpecification createJob(Options options) throws HyracksDataException {

        JobSpecification spec = new JobSpecification(options.frameSize);

        String[] splitNCs = options.ncs.split(",");

        IIndexLifecycleManagerProvider lcManagerProvider = IndexLifecycleManagerProvider.INSTANCE;
        IStorageManagerInterface storageManager = StorageManagerInterface.INSTANCE;

        // schema of tuples coming out of secondary index
        RecordDescriptor secondaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });

        int secondaryFieldCount = 2;
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[secondaryFieldCount];
        secondaryTypeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        secondaryTypeTraits[1] = IntegerPointable.TYPE_TRAITS;

        // comparators for sort fields and BTree fields
        IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[2];
        secondaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);
        secondaryComparatorFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // comparators for primary index
        IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[1];
        primaryComparatorFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // schema of tuples coming out of primary index
        RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(), });

        int primaryFieldCount = 4;
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[primaryFieldCount];
        primaryTypeTraits[0] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[2] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[3] = UTF8StringPointable.TYPE_TRAITS;

        // comparators for btree, note that we only need a comparator for the
        // non-unique key
        // i.e. we will have a range condition on the first field only (implying
        // [-infinity, +infinity] for the second field)
        IBinaryComparatorFactory[] searchComparatorFactories = new IBinaryComparatorFactory[1];
        searchComparatorFactories[0] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);

        // build tuple containing low and high search keys
        ArrayTupleBuilder tb = new ArrayTupleBuilder(searchComparatorFactories.length * 2); // low
        // and
        // high
        // key
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        new UTF8StringSerializerDeserializer().serialize("0", dos); // low
                                                                       // key
        tb.addFieldEndOffset();
        new UTF8StringSerializerDeserializer().serialize("f", dos); // high
                                                                       // key
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer() };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        JobHelper.createPartitionConstraint(spec, keyProviderOp, splitNCs);

        int[] secondaryLowKeyFields = { 0 }; // low key is in field 0 of tuples
                                             // going into secondary index
                                             // search op
        int[] secondaryHighKeyFields = { 1 }; // high key is in field 1 of
                                              // tuples going into secondary
                                              // index search op

        IFileSplitProvider secondarySplitProvider = JobHelper.createFileSplitProvider(splitNCs,
                options.secondaryBTreeName);
        IIndexDataflowHelperFactory dataflowHelperFactory = new BTreeDataflowHelperFactory(true);
        BTreeSearchOperatorDescriptor secondarySearchOp = new BTreeSearchOperatorDescriptor(spec, secondaryRecDesc,
                storageManager, lcManagerProvider, secondarySplitProvider, secondaryTypeTraits,
                searchComparatorFactories, null, secondaryLowKeyFields, secondaryHighKeyFields, true, true,
                dataflowHelperFactory, false, false, null, NoOpOperationCallbackFactory.INSTANCE, null, null);

        JobHelper.createPartitionConstraint(spec, secondarySearchOp, splitNCs);

        // secondary index will output tuples with [UTF8String, Integer]
        // the Integer field refers to the key in the primary index of the
        // source data records
        int[] primaryLowKeyFields = { 1 }; // low key is in field 0 of tuples
                                           // going into primary index search op
        int[] primaryHighKeyFields = { 1 }; // high key is in field 1 of tuples
                                            // going into primary index search
                                            // op

        IFileSplitProvider primarySplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.primaryBTreeName);
        BTreeSearchOperatorDescriptor primarySearchOp = new BTreeSearchOperatorDescriptor(spec, primaryRecDesc,
                storageManager, lcManagerProvider, primarySplitProvider, primaryTypeTraits, primaryComparatorFactories,
                null, primaryLowKeyFields, primaryHighKeyFields, true, true, dataflowHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, null, null);

        JobHelper.createPartitionConstraint(spec, primarySearchOp, splitNCs);

        // have each node print the results of its respective B-Tree
        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        JobHelper.createPartitionConstraint(spec, printer, splitNCs);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, secondarySearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), secondarySearchOp, 0, primarySearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primarySearchOp, 0, printer, 0);

        spec.addRoot(printer);

        return spec;
    }
}