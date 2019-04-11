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
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.examples.btree.helper.BTreeHelperStorageManager;
import org.apache.hyracks.examples.btree.helper.DataGenOperatorDescriptor;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.common.IStorageManager;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

// This example will load a primary index from randomly generated data

public class PrimaryIndexBulkLoadExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)")
        public int port = 1098;

        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;

        @Option(name = "-num-tuples", usage = "Total number of tuples to to be generated for loading", required = true)
        public int numTuples;

        @Option(name = "-btreename", usage = "B-Tree file name", required = true)
        public String btreeName;

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

        // schema of tuples to be generated: 5 fields with string, string, int,
        // int, string
        // we will use field-index 2 as primary key to fill a clustered index
        RecordDescriptor recDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                // this field will not go into B-Tree
                new UTF8StringSerializerDeserializer(),
                // we will use this as payload
                new UTF8StringSerializerDeserializer(),
                // we will use this field as key
                IntegerSerializerDeserializer.INSTANCE,
                // we will use this as payload
                IntegerSerializerDeserializer.INSTANCE,
                // we will use this as payload
                new UTF8StringSerializerDeserializer() });

        // generate numRecords records with field 2 being unique, integer values
        // in [0, 100000], and strings with max length of 10 characters, and
        // random seed 50
        DataGenOperatorDescriptor dataGen =
                new DataGenOperatorDescriptor(spec, recDesc, options.numTuples, 2, 0, 100000, 10, 50);
        // run data generator on first nodecontroller given
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, dataGen, splitNCs[0]);

        // sort the tuples as preparation for bulk load
        // fields to sort on
        int[] sortFields = { 2 };
        // comparators for sort fields
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        ExternalSortOperatorDescriptor sorter =
                new ExternalSortOperatorDescriptor(spec, options.sbSize, sortFields, comparatorFactories, recDesc);
        JobHelper.createPartitionConstraint(spec, sorter, splitNCs);

        // tuples to be put into B-Tree shall have 4 fields
        int fieldCount = 4;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = UTF8StringPointable.TYPE_TRAITS;

        // create providers for B-Tree
        IStorageManager storageManager = BTreeHelperStorageManager.INSTANCE;

        // the B-Tree expects its keyfields to be at the front of its input
        // tuple
        int[] fieldPermutation = { 2, 1, 3, 4 }; // map field 2 of input tuple
                                                 // to field 0 of B-Tree tuple,
                                                 // etc.
        IFileSplitProvider btreeSplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.btreeName);
        IIndexDataflowHelperFactory dataflowHelperFactory =
                new IndexDataflowHelperFactory(storageManager, btreeSplitProvider);
        TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec, recDesc,
                fieldPermutation, 0.7f, false, 1000L, true, dataflowHelperFactory);

        JobHelper.createPartitionConstraint(spec, btreeBulkLoad, splitNCs);

        // distribute the records from the datagen via hashing to the bulk load
        // ops
        IBinaryHashFunctionFactory[] hashFactories = new IBinaryHashFunctionFactory[1];
        hashFactories[0] = PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY);
        IConnectorDescriptor hashConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, hashFactories));
        NullSinkOperatorDescriptor nsOpDesc = new NullSinkOperatorDescriptor(spec);
        JobHelper.createPartitionConstraint(spec, nsOpDesc, splitNCs);

        spec.connect(hashConn, dataGen, 0, sorter, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), btreeBulkLoad, 0, nsOpDesc, 0);
        spec.addRoot(nsOpDesc);

        return spec;
    }
}
