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
import static org.apache.hyracks.examples.tpch.client.Common.lineitemDesc;
import static org.apache.hyracks.examples.tpch.client.Common.lineitemParserFactories;
import static org.apache.hyracks.examples.tpch.client.Common.parseFileSplits;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FrameFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.HashSpillableTableFactory;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.sort.SortGroupByOperatorDescriptor;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * The application client for the performance tests of the groupby
 * operator.
 */
public class Groupby {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)")
        public int port = 1098;

        @Option(name = "-infile-splits", usage = "Comma separated list of file-splits for the input. A file-split is <node-name>:<path>", required = true)
        public String inFileSplits;

        @Option(name = "-outfile-splits", usage = "Comma separated list of file-splits for the output", required = true)
        public String outFileSplits;

        @Option(name = "-input-tuples", usage = "Hash table size ", required = true)
        public int htSize;

        @Option(name = "-input-size", usage = "Physical file size in bytes ", required = true)
        public long fileSize;

        @Option(name = "-frame-size", usage = "Frame size (default: 32768)", required = false)
        public int frameSize = 32768;

        @Option(name = "-frame-limit", usage = "memory limit for sorting (default: 4)", required = false)
        public int frameLimit = 4;

        @Option(name = "-out-plain", usage = "Whether to output plain text (default: true)", required = false)
        public boolean outPlain = true;

        @Option(name = "-algo", usage = "The algorithm to be used: hash|sort", required = true)
        public String algo = "hash";
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

        JobSpecification job;

        long start = System.currentTimeMillis();
        job = createJob(parseFileSplits(options.inFileSplits), parseFileSplits(options.outFileSplits), options.htSize,
                options.fileSize, options.frameLimit, options.frameSize, options.algo, options.outPlain);
        if (job != null) {
            System.out.print("CreateJobTime:" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            JobId jobId = hcc.startJob(job);
            hcc.waitForCompletion(jobId);
            System.out.println("JobExecuteTime:" + (System.currentTimeMillis() - start));
        }
    }

    private static JobSpecification createJob(FileSplit[] inSplits, FileSplit[] outSplits, int htSize, long fileSize,
            int frameLimit, int frameSize, String alg, boolean outPlain) {
        JobSpecification spec = new JobSpecification(frameSize);
        IFileSplitProvider splitsProvider = new ConstantFileSplitProvider(inSplits);

        FileScanOperatorDescriptor fileScanner = new FileScanOperatorDescriptor(spec, splitsProvider,
                new DelimitedDataTupleParserFactory(lineitemParserFactories, '|'), lineitemDesc);

        createPartitionConstraint(spec, fileScanner, inSplits);

        // Output: each unique string with an integer count
        RecordDescriptor outDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                        // IntegerSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE });

        // Specify the grouping key, which will be the string extracted during
        // the scan.
        int[] keys = new int[] { 0,
                // 1
        };

        AbstractOperatorDescriptor grouper;

        if (alg.equalsIgnoreCase("hash")) {// external hash graph
            grouper = new ExternalGroupOperatorDescriptor(spec, htSize, fileSize, keys, frameLimit,
                    new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE },
                    new IntegerNormalizedKeyComputerFactory(),
                    new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
                    new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                            new IntSumFieldAggregatorFactory(keys.length, false) }),
                    outDesc, outDesc, new HashSpillableTableFactory(
                            new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE }));

            createPartitionConstraint(spec, grouper, outSplits);
        } else if (alg.equalsIgnoreCase("sort")) {
            grouper = new SortGroupByOperatorDescriptor(spec, frameLimit, keys, keys,
                    new IntegerNormalizedKeyComputerFactory(),
                    new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE },
                    new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                    new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                            new IntSumFieldAggregatorFactory(keys.length, true) }),
                    outDesc, outDesc, false);

            createPartitionConstraint(spec, grouper, outSplits);
        } else {
            System.err.println("unknow groupby alg:" + alg);
            return null;
        }
        // Connect scanner with the grouper
        IConnectorDescriptor scanGroupConnDef2 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] {
                                // PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY),
                                PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) }));
        spec.connect(scanGroupConnDef2, fileScanner, 0, grouper, 0);

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(outSplits);
        AbstractSingleActivityOperatorDescriptor writer =
                outPlain ? new PlainFileWriterOperatorDescriptor(spec, outSplitProvider, "|")
                        : new FrameFileWriterOperatorDescriptor(spec, outSplitProvider);
        createPartitionConstraint(spec, writer, outSplits);
        IConnectorDescriptor groupOutConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(groupOutConn, grouper, 0, writer, 0);

        spec.addRoot(writer);
        return spec;
    }

}