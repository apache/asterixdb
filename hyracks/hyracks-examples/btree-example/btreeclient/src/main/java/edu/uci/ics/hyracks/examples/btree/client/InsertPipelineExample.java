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

import java.util.UUID;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.ExplicitPartitionConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.TypeTrait;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.BTreeRegistryProvider;
import edu.uci.ics.hyracks.examples.btree.helper.DataGenOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.StorageManagerInterface;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeInsertUpdateDeleteOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.IBTreeRegistryProvider;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOp;
import edu.uci.ics.hyracks.storage.am.btree.tuples.TypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

// This example will insert tuples into the primary and secondary index using an insert pipeline

public class InsertPipelineExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;

        @Option(name = "-num-tuples", usage = "Total number of tuples to to be generated for insertion", required = true)
        public int numTuples;

        @Option(name = "-primary-btreename", usage = "B-Tree file name of primary index", required = true)
        public String primaryBTreeName;

        @Option(name = "-secondary-btreename", usage = "B-Tree file name of secondary index", required = true)
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

    private static JobSpecification createJob(Options options) {

        JobSpecification spec = new JobSpecification();

        String[] splitNCs = options.ncs.split(",");

        // schema of tuples to be generated: 4 fields with int, string, string,
        // string
        // we will use field 2 as primary key to fill a clustered index
        RecordDescriptor recDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, // this field will
                                                           // not go into B-Tree
                UTF8StringSerializerDeserializer.INSTANCE, // we will use this
                                                           // as payload
                IntegerSerializerDeserializer.INSTANCE, // we will use this
                                                        // field as key
                IntegerSerializerDeserializer.INSTANCE, // we will use this as
                                                        // payload
                UTF8StringSerializerDeserializer.INSTANCE // we will use this as
                                                          // payload
                });

        // generate numRecords records with field 2 being unique, integer values
        // in [0, 100000], and strings with max length of 10 characters, and
        // random seed 100
        DataGenOperatorDescriptor dataGen = new DataGenOperatorDescriptor(spec, recDesc, options.numTuples, 2, 0,
                100000, 10, 100);
        // run data generator on first nodecontroller given
        PartitionConstraint dataGenConstraint = new ExplicitPartitionConstraint(
                new LocationConstraint[] { new AbsoluteLocationConstraint(splitNCs[0]) });
        dataGen.setPartitionConstraint(dataGenConstraint);

        IBTreeRegistryProvider btreeRegistryProvider = BTreeRegistryProvider.INSTANCE;
        IStorageManagerInterface storageManager = StorageManagerInterface.INSTANCE;

        // prepare insertion into primary index
        // tuples to be put into B-Tree shall have 4 fields
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

        // the B-Tree expects its keyfields to be at the front of its input
        // tuple
        int[] primaryFieldPermutation = { 2, 1, 3, 4 }; // map field 2 of input
                                                        // tuple to field 0 of
                                                        // B-Tree tuple, etc.
        // comparator factories for primary index
        IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[1];
        primaryComparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        IFileSplitProvider primarySplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.primaryBTreeName);

        // create operator descriptor
        BTreeInsertUpdateDeleteOperatorDescriptor primaryInsert = new BTreeInsertUpdateDeleteOperatorDescriptor(spec,
                recDesc, storageManager, btreeRegistryProvider, primarySplitProvider, primaryInteriorFrameFactory,
                primaryLeafFrameFactory, primaryTypeTraits, primaryComparatorFactories, primaryFieldPermutation,
                BTreeOp.BTO_INSERT);
        PartitionConstraint primaryInsertConstraint = JobHelper.createPartitionConstraint(splitNCs);
        primaryInsert.setPartitionConstraint(primaryInsertConstraint);

        // prepare insertion into secondary index
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
        int[] secondaryFieldPermutation = { 1, 2 };
        // comparator factories for primary index
        IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[2];
        secondaryComparatorFactories[0] = UTF8StringBinaryComparatorFactory.INSTANCE;
        secondaryComparatorFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        IFileSplitProvider secondarySplitProvider = JobHelper.createFileSplitProvider(splitNCs,
                options.secondaryBTreeName);
        // create operator descriptor
        BTreeInsertUpdateDeleteOperatorDescriptor secondaryInsert = new BTreeInsertUpdateDeleteOperatorDescriptor(spec,
                recDesc, storageManager, btreeRegistryProvider, secondarySplitProvider, secondaryInteriorFrameFactory,
                secondaryLeafFrameFactory, secondaryTypeTraits, secondaryComparatorFactories,
                secondaryFieldPermutation, BTreeOp.BTO_INSERT);
        PartitionConstraint secondaryInsertConstraint = JobHelper.createPartitionConstraint(splitNCs);
        secondaryInsert.setPartitionConstraint(secondaryInsertConstraint);

        // end the insert pipeline at this sink operator
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        PartitionConstraint nullSinkPartitionConstraint = JobHelper.createPartitionConstraint(splitNCs);
        nullSink.setPartitionConstraint(nullSinkPartitionConstraint);

        // distribute the records from the datagen via hashing to the bulk load
        // ops
        IBinaryHashFunctionFactory[] hashFactories = new IBinaryHashFunctionFactory[1];
        hashFactories[0] = UTF8StringBinaryHashFunctionFactory.INSTANCE;
        IConnectorDescriptor hashConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, hashFactories));

        // connect the ops

        spec.connect(hashConn, dataGen, 0, primaryInsert, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primaryInsert, 0, secondaryInsert, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryInsert, 0, nullSink, 0);

        spec.addRoot(nullSink);

        return spec;
    }
}
