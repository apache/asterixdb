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
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.DataGenOperatorDescriptor;
import edu.uci.ics.hyracks.examples.btree.helper.IndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.examples.btree.helper.StorageManagerInterface;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexInsertUpdateDeleteOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

// This example will insert tuples into the primary and secondary index using an insert pipeline

public class InsertPipelineExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)")
        public int port = 1098;

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
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dataGen, splitNCs[0]);

        IIndexLifecycleManagerProvider lcManagerProvider = IndexLifecycleManagerProvider.INSTANCE;
        IStorageManagerInterface storageManager = StorageManagerInterface.INSTANCE;

        // prepare insertion into primary index
        // tuples to be put into B-Tree shall have 4 fields
        int primaryFieldCount = 4;
        ITypeTraits[] primaryTypeTraits = new ITypeTraits[primaryFieldCount];
        primaryTypeTraits[0] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        primaryTypeTraits[2] = IntegerPointable.TYPE_TRAITS;
        primaryTypeTraits[3] = UTF8StringPointable.TYPE_TRAITS;

        // comparator factories for primary index
        IBinaryComparatorFactory[] primaryComparatorFactories = new IBinaryComparatorFactory[1];
        primaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // the B-Tree expects its keyfields to be at the front of its input
        // tuple
        int[] primaryFieldPermutation = { 2, 1, 3, 4 }; // map field 2 of input
                                                        // tuple to field 0 of
                                                        // B-Tree tuple, etc.        
        IFileSplitProvider primarySplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.primaryBTreeName);

        IIndexDataflowHelperFactory dataflowHelperFactory = new BTreeDataflowHelperFactory();

        // create operator descriptor
        TreeIndexInsertUpdateDeleteOperatorDescriptor primaryInsert = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, recDesc, storageManager, lcManagerProvider, primarySplitProvider, primaryTypeTraits,
                primaryComparatorFactories, null, primaryFieldPermutation, IndexOperation.INSERT,
                dataflowHelperFactory, null, NoOpOperationCallbackFactory.INSTANCE);
        JobHelper.createPartitionConstraint(spec, primaryInsert, splitNCs);

        // prepare insertion into secondary index
        // tuples to be put into B-Tree shall have 2 fields
        int secondaryFieldCount = 2;
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[secondaryFieldCount];
        secondaryTypeTraits[0] = UTF8StringPointable.TYPE_TRAITS;
        secondaryTypeTraits[1] = IntegerPointable.TYPE_TRAITS;

        // comparator factories for secondary index
        IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[2];
        secondaryComparatorFactories[0] = PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY);
        secondaryComparatorFactories[1] = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY);

        // the B-Tree expects its keyfields to be at the front of its input
        // tuple
        int[] secondaryFieldPermutation = { 1, 2 };
        IFileSplitProvider secondarySplitProvider = JobHelper.createFileSplitProvider(splitNCs,
                options.secondaryBTreeName);
        // create operator descriptor
        TreeIndexInsertUpdateDeleteOperatorDescriptor secondaryInsert = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, recDesc, storageManager, lcManagerProvider, secondarySplitProvider, secondaryTypeTraits,
                secondaryComparatorFactories, null, secondaryFieldPermutation, IndexOperation.INSERT,
                dataflowHelperFactory, null, NoOpOperationCallbackFactory.INSTANCE);
        JobHelper.createPartitionConstraint(spec, secondaryInsert, splitNCs);

        // end the insert pipeline at this sink operator
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        JobHelper.createPartitionConstraint(spec, nullSink, splitNCs);

        // distribute the records from the datagen via hashing to the bulk load
        // ops
        IBinaryHashFunctionFactory[] hashFactories = new IBinaryHashFunctionFactory[1];
        hashFactories[0] = PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY);
        IConnectorDescriptor hashConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, hashFactories));

        // connect the ops

        spec.connect(hashConn, dataGen, 0, primaryInsert, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), primaryInsert, 0, secondaryInsert, 0);

        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryInsert, 0, nullSink, 0);

        spec.addRoot(nullSink);

        return spec;
    }
}