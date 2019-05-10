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
package org.apache.hyracks.examples.tpch.client;

import static org.apache.hyracks.examples.tpch.client.Common.createPartitionConstraint;
import static org.apache.hyracks.examples.tpch.client.Common.custParserFactories;
import static org.apache.hyracks.examples.tpch.client.Common.orderParserFactories;
import static org.apache.hyracks.examples.tpch.client.Common.parseFileSplits;

import java.util.EnumSet;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryHashFunctionFamily;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.HashSpillableTableFactory;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.FloatSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.InMemoryHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.JoinComparatorFactory;
import org.apache.hyracks.dataflow.std.join.NestedLoopJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Join {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)", required = false)
        public int port = 1098;

        @Option(name = "-infile-customer-splits", usage = "Comma separated list of file-splits for the CUSTOMER input. A file-split is <node-name>:<path>", required = true)
        public String inFileCustomerSplits;

        @Option(name = "-infile-order-splits", usage = "Comma separated list of file-splits for the ORDER input. A file-split is <node-name>:<path>", required = true)
        public String inFileOrderSplits;

        @Option(name = "-outfile-splits", usage = "Comma separated list of file-splits for the output", required = true)
        public String outFileSplits;

        @Option(name = "-num-join-partitions", usage = "Number of Join partitions to use (default: 1)", required = false)
        public int numJoinPartitions = 1;

        @Option(name = "-profile", usage = "Enable/Disable profiling. (default: enabled)")
        public boolean profile = true;

        @Option(name = "-table-size", usage = "Table size for in-memory hash join. (default: 8191)", required = false)
        public int tableSize = 8191;

        @Option(name = "-algo", usage = "Join types:InMem|NestedLoop|Hybrid|Grace", required = true)
        public String algo;

        // For grace/hybrid hash join only
        @Option(name = "-mem-size", usage = "Memory size for hash join", required = true)
        public int memSize;

        @Option(name = "-input-size", usage = "Input size of the hybrid hash join", required = false)
        public int graceInputSize = 100000;

        @Option(name = "-records-per-frame", usage = "Records per frame for hybrid hash join", required = false)
        public int graceRecordsPerFrame = 200;

        @Option(name = "-grace-factor", usage = "Factor of the grace/hybrid hash join, (default:1.2)", required = false)
        public double graceFactor = 1.2;

        // Whether group-by is processed after the join
        @Option(name = "-has-groupby", usage = "Whether to have group-by operation after join (default: disabled)", required = false)
        public boolean hasGroupBy = false;

        @Option(name = "-frame-size", usage = "Hyracks frame size (default: 32768)", required = false)
        public int frameSize = 32768;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        if (args.length == 0) {
            parser.printUsage(System.err);
            return;
        }
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksConnection(options.host, options.port);

        JobSpecification job = createJob(parseFileSplits(options.inFileCustomerSplits),
                parseFileSplits(options.inFileOrderSplits), parseFileSplits(options.outFileSplits),
                options.numJoinPartitions, options.algo, options.graceInputSize, options.graceRecordsPerFrame,
                options.graceFactor, options.memSize, options.tableSize, options.hasGroupBy, options.frameSize);
        if (job == null) {
            return;
        }

        long start = System.currentTimeMillis();
        JobId jobId = hcc.startJob(job,
                options.profile ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }

    private static JobSpecification createJob(FileSplit[] customerSplits, FileSplit[] orderSplits,
            FileSplit[] resultSplits, int numJoinPartitions, String algo, int graceInputSize, int graceRecordsPerFrame,
            double graceFactor, int memSize, int tableSize, boolean hasGroupBy, int frameSize)
            throws HyracksDataException {
        JobSpecification spec = new JobSpecification(frameSize);

        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(customerSplits);
        long custFileSize = 0;
        for (int i = 0; i < customerSplits.length; i++) {
            custFileSize += customerSplits[i].getFile(null).length();
        }

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(orderSplits);
        long orderFileSize = 0;
        for (int i = 0; i < orderSplits.length; i++) {
            orderFileSize += orderSplits[i].getFile(null).length();
        }

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderParserFactories, '|'), Common.ordersDesc);
        createPartitionConstraint(spec, ordScanner, orderSplits);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custParserFactories, '|'), Common.custDesc);
        createPartitionConstraint(spec, custScanner, customerSplits);

        IOperatorDescriptor join;

        if ("nestedloop".equalsIgnoreCase(algo)) {
            join = new NestedLoopJoinOperatorDescriptor(spec,
                    new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1),
                    Common.custOrderJoinDesc, memSize, false, null);

        } else if ("inmem".equalsIgnoreCase(algo)) {
            join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 0 }, new int[] { 1 },
                    new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                    new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                    new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1),
                    Common.custOrderJoinDesc, tableSize, null, memSize * frameSize);

        } else if ("hybrid".equalsIgnoreCase(algo)) {
            join = new OptimizedHybridHashJoinOperatorDescriptor(spec, memSize, graceInputSize, graceFactor,
                    new int[] { 0 }, new int[] { 1 },
                    new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                    new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                    Common.custOrderJoinDesc,
                    new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1),
                    new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), null);

        } else {
            System.err.println("unknown algorithm:" + algo);
            return null;
        }

        PartitionConstraintHelper.addPartitionCountConstraint(spec, join, numJoinPartitions);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IOperatorDescriptor endingOp = join;

        if (hasGroupBy) {

            RecordDescriptor groupResultDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                    new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });

            ExternalGroupOperatorDescriptor gby = new ExternalGroupOperatorDescriptor(spec, tableSize,
                    custFileSize + orderFileSize, new int[] { 6 }, memSize,
                    new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                    new UTF8StringNormalizedKeyComputerFactory(),
                    new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                            new IntSumFieldAggregatorFactory(1, false), new IntSumFieldAggregatorFactory(3, false),
                            new FloatSumFieldAggregatorFactory(5, false) }),
                    new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                            new IntSumFieldAggregatorFactory(1, false), new IntSumFieldAggregatorFactory(2, false),
                            new FloatSumFieldAggregatorFactory(3, false) }),
                    groupResultDesc, groupResultDesc, new HashSpillableTableFactory(
                            new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE }));

            createPartitionConstraint(spec, gby, resultSplits);

            IConnectorDescriptor joinGroupConn = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(new int[] { 6 }, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
            spec.connect(joinGroupConn, join, 0, gby, 0);

            endingOp = gby;
        }

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(resultSplits);
        //FrameFileWriterOperatorDescriptor writer = new FrameFileWriterOperatorDescriptor(spec, outSplitProvider);
        IOperatorDescriptor writer = new PlainFileWriterOperatorDescriptor(spec, outSplitProvider, "|");

        createPartitionConstraint(spec, writer, resultSplits);

        IConnectorDescriptor endingPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(endingPrinterConn, endingOp, 0, writer, 0);

        spec.addRoot(writer);
        return spec;
    }
}
