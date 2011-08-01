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

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
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

// This example will perform an ordered scan on the primary index
// i.e. a range-search for [-infinity, +infinity]

public class PrimaryIndexSearchExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;

        @Option(name = "-btreename", usage = "B-Tree file name to search", required = true)
        public String btreeName;
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

    private static JobSpecification createJob(Options options) throws HyracksDataException {

        JobSpecification spec = new JobSpecification();

        String[] splitNCs = options.ncs.split(",");

        int fieldCount = 4;
        ITypeTrait[] typeTraits = new ITypeTrait[fieldCount];
        typeTraits[0] = new TypeTrait(4);
        typeTraits[1] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);
        typeTraits[2] = new TypeTrait(4);
        typeTraits[3] = new TypeTrait(ITypeTrait.VARIABLE_LENGTH);

        // create factories and providers for B-Tree
        TypeAwareTupleWriterFactory tupleWriterFactory = new TypeAwareTupleWriterFactory(typeTraits);
        IBTreeInteriorFrameFactory interiorFrameFactory = new NSMInteriorFrameFactory(tupleWriterFactory);
        IBTreeLeafFrameFactory leafFrameFactory = new NSMLeafFrameFactory(tupleWriterFactory);
        IBTreeRegistryProvider btreeRegistryProvider = BTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = StorageManagerInterface.INSTANCE;

        // schema of tuples coming out of primary index
        RecordDescriptor recDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE, });

        // comparators for btree
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        // build tuple containing low and high search keys
        ArrayTupleBuilder tb = new ArrayTupleBuilder(comparatorFactories.length * 2); // high
                                                                                      // key
                                                                                      // and
                                                                                      // low
                                                                                      // key
        DataOutput dos = tb.getDataOutput();

        tb.reset();
        IntegerSerializerDeserializer.INSTANCE.serialize(100, dos); // low key
        tb.addFieldEndOffset();
        IntegerSerializerDeserializer.INSTANCE.serialize(200, dos); // build
                                                                    // high key
        tb.addFieldEndOffset();

        ISerializerDeserializer[] keyRecDescSers = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        JobHelper.createPartitionConstraint(spec, keyProviderOp, splitNCs);

        int[] lowKeyFields = { 0 }; // low key is in field 0 of tuples going
                                    // into search op
        int[] highKeyFields = { 1 }; // low key is in field 1 of tuples going
                                     // into search op

        IFileSplitProvider btreeSplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.btreeName);
        BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(spec, recDesc, storageManager,
                btreeRegistryProvider, btreeSplitProvider, interiorFrameFactory, leafFrameFactory, typeTraits,
                comparatorFactories, true, lowKeyFields, highKeyFields, true, true);
        JobHelper.createPartitionConstraint(spec, btreeSearchOp, splitNCs);

        // have each node print the results of its respective B-Tree
        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        JobHelper.createPartitionConstraint(spec, printer, splitNCs);

        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, btreeSearchOp, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), btreeSearchOp, 0, printer, 0);

        spec.addRoot(printer);

        return spec;
    }
}