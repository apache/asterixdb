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

import java.io.DataOutput;
import java.util.UUID;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.BTreeRegistryProvider;
import edu.uci.ics.hyracks.examples.btree.helper.StorageManagerInterface;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

// This example will perform range search on the secondary index
// and then retrieve the corresponding source records from the primary index

public class SecondaryIndexSearchExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;

        @Option(name = "-primary-btreename", usage = "Primary B-Tree file name", required = true)
        public String primaryBTreeName;

        @Option(name = "-secondary-btreename", usage = "Secondary B-Tree file name to search", required = true)
        public String secondaryBTreeName;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job = createJob(options);

        long start = System.currentTimeMillis();
        UUID jobId = hcc.createJob(options.app, job);
        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }

    private static JobSpecification createJob(Options options) throws HyracksDataException {

        JobSpecification spec = new JobSpecification();

        String[] splitNCs = options.ncs.split(",");

        IBTreeRegistryProvider btreeRegistryProvider = BTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = StorageManagerInterface.INSTANCE;

        // schema of tuples coming out of secondary index
        RecordDescriptor secondaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        int secondaryFieldCount = 2;
        ITypeTrait[] secondaryTypeTraits = new ITypeTrait[secondaryFieldCount];
        secondaryTypeTraits[0] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        secondaryTypeTraits[1] = new TypeTrait(4);

        // create factories and providers for secondary B-Tree
        TypeAwareTupleWriterFactory secondaryTupleWriterFactory = new TypeAwareTupleWriterFactory(secondaryTypeTraits);
        IBTreeInteriorFrameFactory secondaryInteriorFrameFactory = new NSMInteriorFrameFactory(
                secondaryTupleWriterFactory);
        IBTreeLeafFrameFactory secondaryLeafFrameFactory = new NSMLeafFrameFactory(secondaryTupleWriterFactory);

        // schema of tuples coming out of primary index
        RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE, });

        int primaryFieldCount = 4;
        ITypeTrait[] primaryTypeTraits = new ITypeTrait[primaryFieldCount];
        primaryTypeTraits[0] = new TypeTrait(4);
        primaryTypeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        primaryTypeTraits[2] = new TypeTrait(4);
        primaryTypeTraits[3] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);

        // create factories and providers for secondary B-Tree
        TypeAwareTupleWriterFactory primaryTupleWriterFactory = new TypeAwareTupleWriterFactory(primaryTypeTraits);
        IBTreeInteriorFrameFactory primaryInteriorFrameFactory = new NSMInteriorFrameFactory(primaryTupleWriterFactory);
        IBTreeLeafFrameFactory primaryLeafFrameFactory = new NSMLeafFrameFactory(primaryTupleWriterFactory);

        // comparators for btree, note that we only need a comparator for the
        // non-unique key
        // i.e. we will have a range condition on the first field only (implying
        // [-infinity, +infinity] for the second field)
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;

        // build tuple containing low and high search keys
        ArrayTupleBuilder tb = new ArrayTupleBuilder(comparatorFactories.length * 2); // low
                                                                                      // and
                                                                                      // high
                                                                                      // key
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize("0", dos); // low
                                                                       // key
        tb.addFieldEndOffset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize("f", dos); // high
                                                                       // key
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        PartitionConstraint keyProviderPartitionConstraint = JobHelper.createPartitionConstraint(splitNCs);
        keyProviderOp.setPartitionConstraint(keyProviderPartitionConstraint);

        int[] secondaryLowKeyFields = { 0 }; // low key is in field 0 of tuples
                                             // going into secondary index
                                             // search op
        int[] secondaryHighKeyFields = { 1 }; // high key is in field 1 of
                                              // tuples going into secondary
                                              // index search op

        IFileSplitProvider secondarySplitProvider = JobHelper.createFileSplitProvider(splitNCs,
                options.secondaryBTreeName);
        BTreeSearchOperatorDescriptor secondarySearchOp = new BTreeSearchOperatorDescriptor(spec, secondaryRecDesc,
                storageManager, btreeRegistryProvider, secondarySplitProvider, secondaryInteriorFrameFactory,
                secondaryLeafFrameFactory, secondaryTypeTraits, comparatorFactories, true, secondaryLowKeyFields,
                secondaryHighKeyFields, true, true);
        PartitionConstraint secondarySearchConstraint = JobHelper.createPartitionConstraint(splitNCs);
        secondarySearchOp.setPartitionConstraint(secondarySearchConstraint);

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
                storageManager, btreeRegistryProvider, primarySplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, comparatorFactories, true, primaryLowKeyFields,
                primaryHighKeyFields, true, true);
        PartitionConstraint primarySearchConstraint = JobHelper.createPartitionConstraint(splitNCs);
        primarySearchOp.setPartitionConstraint(primarySearchConstraint);

        // have each node print the results of its respective B-Tree
        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerConstraint = JobHelper.createPartitionConstraint(splitNCs);
        printer.setPartitionConstraint(printerConstraint);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, secondarySearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), secondarySearchOp, 0, primarySearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primarySearchOp, 0, printer, 0);

        spec.addRoot(printer);

        return spec;
    }
}