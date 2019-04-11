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

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import org.apache.hyracks.examples.btree.helper.BTreeHelperStorageManager;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

// This example will perform an ordered scan on the primary index
// i.e. a range-search for [-infinity, +infinity]

public class PrimaryIndexSearchExample {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)")
        public int port = 1098;

        @Option(name = "-target-ncs", usage = "Comma separated list of node-controller names to use", required = true)
        public String ncs;

        @Option(name = "-btreename", usage = "B-Tree file name to search", required = true)
        public String btreeName;

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

        int fieldCount = 4;
        ITypeTraits[] typeTraits = new ITypeTraits[fieldCount];
        typeTraits[0] = IntegerPointable.TYPE_TRAITS;
        typeTraits[1] = UTF8StringPointable.TYPE_TRAITS;
        typeTraits[2] = IntegerPointable.TYPE_TRAITS;
        typeTraits[3] = UTF8StringPointable.TYPE_TRAITS;

        // comparators for btree
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        // create roviders for B-Tree
        IStorageManager storageManager = BTreeHelperStorageManager.INSTANCE;

        // schema of tuples coming out of primary index
        RecordDescriptor recDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(), });

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

        ISerializerDeserializer[] keyRecDescSers =
                { new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);

        ConstantTupleSourceOperatorDescriptor keyProviderOp = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        JobHelper.createPartitionConstraint(spec, keyProviderOp, splitNCs);

        int[] lowKeyFields = { 0 }; // low key is in field 0 of tuples going
                                    // into search op
        int[] highKeyFields = { 1 }; // low key is in field 1 of tuples going
                                     // into search op

        IFileSplitProvider btreeSplitProvider = JobHelper.createFileSplitProvider(splitNCs, options.btreeName);
        IIndexDataflowHelperFactory dataflowHelperFactory =
                new IndexDataflowHelperFactory(storageManager, btreeSplitProvider);
        BTreeSearchOperatorDescriptor btreeSearchOp = new BTreeSearchOperatorDescriptor(spec, recDesc, lowKeyFields,
                highKeyFields, true, true, dataflowHelperFactory, false, false, null,
                NoOpOperationCallbackFactory.INSTANCE, null, null, false);

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
