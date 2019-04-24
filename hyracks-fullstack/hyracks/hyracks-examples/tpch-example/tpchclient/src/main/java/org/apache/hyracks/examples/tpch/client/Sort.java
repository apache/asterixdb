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
import static org.apache.hyracks.examples.tpch.client.Common.orderParserFactories;
import static org.apache.hyracks.examples.tpch.client.Common.ordersDesc;
import static org.apache.hyracks.examples.tpch.client.Common.parseFileSplits;

import java.util.EnumSet;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.buffermanager.EnumFreeSlotPolicy;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.AbstractSorterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.Algorithm;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.TopKSorterOperatorDescriptor;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Sort {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)", required = false)
        public int port = 1098;

        @Option(name = "-frame-size", usage = "Hyracks frame size (default: 32768)", required = false)
        public int frameSize = 32768;

        @Option(name = "-frame-limit", usage = "memory limit for sorting (default: 4)", required = false)
        public int frameLimit = 4;

        @Option(name = "-infile-splits", usage = "Comma separated list of file-splits for the ORDER input. A file-split is <node-name>:<path>", required = true)
        public String inFileOrderSplits;

        @Option(name = "-outfile-splits", usage = "Comma separated list of file-splits for the output", required = true)
        public String outFileSplits;

        @Option(name = "-membuffer-alg", usage = "bestfit or lastfit (default: lastfit)", required = false)
        public String memBufferAlg = "lastfit";

        @Option(name = "-profile", usage = "Enable/Disable profiling. (default: enabled)")
        public boolean profile = true;

        @Option(name = "-topK", usage = "only output topK for each node. (default: not set)")
        public int topK = Integer.MAX_VALUE;

        @Option(name = "-heapSort", usage = "using heap sort for topK result. (default: false)")
        public boolean usingHeapSorter = false;
    }

    static int[] SortFields = new int[] { 1, 0 };
    static IBinaryComparatorFactory[] SortFieldsComparatorFactories = new IBinaryComparatorFactory[] {
            UTF8StringBinaryComparatorFactory.INSTANCE, UTF8StringBinaryComparatorFactory.INSTANCE };

    static IBinaryHashFunctionFactory[] orderBinaryHashFunctionFactories =
            new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                    PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) };

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        if (args.length == 0) {
            parser.printUsage(System.err);
            return;
        }
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksConnection(options.host, options.port);

        JobSpecification job = createJob(parseFileSplits(options.inFileOrderSplits),
                parseFileSplits(options.outFileSplits), options.memBufferAlg, options.frameLimit, options.frameSize,
                options.topK, options.usingHeapSorter);

        long start = System.currentTimeMillis();
        JobId jobId = hcc.startJob(job,
                options.profile ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println("finished in:" + (end - start) + "ms");
    }

    private static JobSpecification createJob(FileSplit[] ordersSplits, FileSplit[] outputSplit, String memBufferAlg,
            int frameLimit, int frameSize, int limit, boolean usingHeapSorter) {
        JobSpecification spec = new JobSpecification();

        spec.setFrameSize(frameSize);
        IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(ordersSplits);
        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitProvider,
                new DelimitedDataTupleParserFactory(orderParserFactories, '|'), ordersDesc);
        createPartitionConstraint(spec, ordScanner, ordersSplits);
        AbstractSorterOperatorDescriptor sorter;
        if (usingHeapSorter && limit < Integer.MAX_VALUE) {
            sorter = new TopKSorterOperatorDescriptor(spec, frameLimit, limit, SortFields,
                    (INormalizedKeyComputerFactory) null, SortFieldsComparatorFactories, ordersDesc);
        } else {
            if (memBufferAlg.equalsIgnoreCase("bestfit")) {
                sorter = new ExternalSortOperatorDescriptor(spec, frameLimit, SortFields, null,
                        SortFieldsComparatorFactories, ordersDesc, Algorithm.MERGE_SORT,
                        EnumFreeSlotPolicy.SMALLEST_FIT, limit);
            } else if (memBufferAlg.equalsIgnoreCase("biggestfit")) {
                sorter = new ExternalSortOperatorDescriptor(spec, frameLimit, SortFields, null,
                        SortFieldsComparatorFactories, ordersDesc, Algorithm.MERGE_SORT, EnumFreeSlotPolicy.BIGGEST_FIT,
                        limit);
            } else {
                sorter = new ExternalSortOperatorDescriptor(spec, frameLimit, SortFields, null,
                        SortFieldsComparatorFactories, ordersDesc, Algorithm.MERGE_SORT, EnumFreeSlotPolicy.LAST_FIT,
                        limit);

            }
        }
        createPartitionConstraint(spec, sorter, ordersSplits);
        IFileSplitProvider outputSplitProvider = new ConstantFileSplitProvider(outputSplit);
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outputSplitProvider, "|");
        createPartitionConstraint(spec, printer, outputSplit);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);

        spec.connect(
                new MToNPartitioningMergingConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(SortFields, orderBinaryHashFunctionFactories), SortFields,
                        SortFieldsComparatorFactories, new UTF8StringNormalizedKeyComputerFactory()),
                sorter, 0, printer, 0);

        spec.addRoot(printer);
        return spec;
    }
}
