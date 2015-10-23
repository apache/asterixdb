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

import static org.apache.hyracks.examples.tpch.client.Common.*;

import java.util.EnumSet;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.FrameFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.hash.HashGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.GraceHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.HybridHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.InMemoryHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.NestedLoopJoinOperatorDescriptor;

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

        @Option(name = "-table-size", usage = "Table size for in-memory hash join", required = false)
        public int tableSize = 8191;

        @Option(name = "-algo", usage = "Join types", required = true)
        public String algo;

        // For grace/hybrid hash join only
        @Option(name = "-mem-size", usage = "Memory size for hash join", required = true)
        public int memSize;

        @Option(name = "-input-size", usage = "Input size of the grace/hybrid hash join", required = false)
        public int graceInputSize = 10;

        @Option(name = "-records-per-frame", usage = "Records per frame for grace/hybrid hash join", required = false)
        public int graceRecordsPerFrame = 200;

        @Option(name = "-grace-factor", usage = "Factor of the grace/hybrid hash join", required = false)
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
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksConnection(options.host, options.port);

        JobSpecification job = createJob(parseFileSplits(options.inFileCustomerSplits),
                parseFileSplits(options.inFileOrderSplits), parseFileSplits(options.outFileSplits),
                options.numJoinPartitions, options.algo, options.graceInputSize, options.graceRecordsPerFrame,
                options.graceFactor, options.memSize, options.tableSize, options.hasGroupBy, options.frameSize);

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

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(orderSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), Common.ordersDesc);
        createPartitionConstraint(spec, ordScanner, orderSplits);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), Common.custDesc);
        createPartitionConstraint(spec, custScanner, customerSplits);

        IOperatorDescriptor join;

        if ("nestedloop".equalsIgnoreCase(algo)) {
            join = new NestedLoopJoinOperatorDescriptor(spec, new JoinComparatorFactory(
                    PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1), Common.custOrderJoinDesc,
                    memSize, false, null);

        } else if ("gracehash".equalsIgnoreCase(algo)) {
            join = new GraceHashJoinOperatorDescriptor(
                    spec,
                    memSize,
                    graceInputSize,
                    graceRecordsPerFrame,
                    graceFactor,
                    new int[] { 0 },
                    new int[] { 1 },
                    new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                            .of(UTF8StringPointable.FACTORY) },
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    Common.custOrderJoinDesc, null);

        } else if ("hybridhash".equalsIgnoreCase(algo)) {
            join = new HybridHashJoinOperatorDescriptor(
                    spec,
                    memSize,
                    graceInputSize,
                    graceRecordsPerFrame,
                    graceFactor,
                    new int[] { 0 },
                    new int[] { 1 },
                    new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                            .of(UTF8StringPointable.FACTORY) },
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    Common.custOrderJoinDesc, null);

        } else {
            join = new InMemoryHashJoinOperatorDescriptor(
                    spec,
                    new int[] { 0 },
                    new int[] { 1 },
                    new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                            .of(UTF8StringPointable.FACTORY) },
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    Common.custOrderJoinDesc, 6000000, null);
        }

        PartitionConstraintHelper.addPartitionCountConstraint(spec, join, numJoinPartitions);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IOperatorDescriptor endingOp = join;

        if (hasGroupBy) {

            RecordDescriptor groupResultDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                    new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });

            HashGroupOperatorDescriptor gby = new HashGroupOperatorDescriptor(
                    spec,
                    new int[] { 6 },
                    new FieldHashPartitionComputerFactory(new int[] { 6 },
                            new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                    .of(UTF8StringPointable.FACTORY) }),
                    new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                    new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                    groupResultDesc, 16);
            createPartitionConstraint(spec, gby, resultSplits);

            IConnectorDescriptor joinGroupConn = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(new int[] { 6 },
                            new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                    .of(UTF8StringPointable.FACTORY) }));
            spec.connect(joinGroupConn, join, 0, gby, 0);

            endingOp = gby;
        }

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(resultSplits);
        FrameFileWriterOperatorDescriptor writer = new FrameFileWriterOperatorDescriptor(spec, outSplitProvider);
        createPartitionConstraint(spec, writer, resultSplits);

        IConnectorDescriptor endingPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(endingPrinterConn, endingOp, 0, writer, 0);

        spec.addRoot(writer);
        return spec;
    }



    static class JoinComparatorFactory implements ITuplePairComparatorFactory {
        private static final long serialVersionUID = 1L;

        private final IBinaryComparatorFactory bFactory;
        private final int pos0;
        private final int pos1;

        public JoinComparatorFactory(IBinaryComparatorFactory bFactory, int pos0, int pos1) {
            this.bFactory = bFactory;
            this.pos0 = pos0;
            this.pos1 = pos1;
        }

        @Override
        public ITuplePairComparator createTuplePairComparator(IHyracksTaskContext ctx) {
            return new JoinComparator(bFactory.createBinaryComparator(), pos0, pos1);
        }
    }

    static class JoinComparator implements ITuplePairComparator {

        private final IBinaryComparator bComparator;
        private final int field0;
        private final int field1;

        public JoinComparator(IBinaryComparator bComparator, int field0, int field1) {
            this.bComparator = bComparator;
            this.field0 = field0;
            this.field1 = field1;
        }

        @Override
        public int compare(IFrameTupleAccessor accessor0, int tIndex0, IFrameTupleAccessor accessor1, int tIndex1)
                throws HyracksDataException {
            int tStart0 = accessor0.getTupleStartOffset(tIndex0);
            int fStartOffset0 = accessor0.getFieldSlotsLength() + tStart0;

            int tStart1 = accessor1.getTupleStartOffset(tIndex1);
            int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

            int fStart0 = accessor0.getFieldStartOffset(tIndex0, field0);
            int fEnd0 = accessor0.getFieldEndOffset(tIndex0, field0);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = accessor1.getFieldStartOffset(tIndex1, field1);
            int fEnd1 = accessor1.getFieldEndOffset(tIndex1, field1);
            int fLen1 = fEnd1 - fStart1;

            int c = bComparator.compare(accessor0.getBuffer().array(), fStart0 + fStartOffset0, fLen0, accessor1
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
            return 0;
        }
    }
}