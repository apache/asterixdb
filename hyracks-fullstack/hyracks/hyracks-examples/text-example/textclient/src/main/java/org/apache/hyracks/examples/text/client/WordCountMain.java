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
package org.apache.hyracks.examples.text.client;

import java.io.File;
import java.util.EnumSet;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.UnmanagedFileSplit;
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
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FrameFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.HashSpillableTableFactory;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.FloatSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;
import org.apache.hyracks.examples.text.WordTupleParserFactory;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class WordCountMain {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1098)")
        public int port = 1098;

        @Option(name = "-infile-splits", usage = "Comma separated list of file-splits for the input. A file-split is <node-name>:<path>", required = true)
        public String inFileSplits;

        @Option(name = "-outfile-splits", usage = "Comma separated list of file-splits for the output", required = true)
        public String outFileSplits;

        @Option(name = "-algo", usage = "Use Hash based grouping", required = true)
        public String algo;

        @Option(name = "-format", usage = "Specify output format: binary/text (default: text)", required = false)
        public String format = "text";

        @Option(name = "-hashtable-size", usage = "Hash table size (default: 8191)", required = false)
        public int htSize = 8191;

        @Option(name = "-frame-limit", usage = "Memory limit in frames (default:4)", required = false)
        public int memFrameLimit = 10;

        @Option(name = "-runtime-profiling", usage = "Indicates if runtime profiling should be enabled. (default: false)")
        public boolean runtimeProfiling = false;

        @Option(name = "-frame-size", usage = "Hyracks frame size (default: 32768)", required = false)
        public int frameSize = 32768;
    }

    private static long fileSize = 0;

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksConnection(options.host, options.port);

        JobSpecification job = createJob(parseFileSplits(options.inFileSplits), parseFileSplits(options.outFileSplits),
                options.algo, options.htSize, options.memFrameLimit, options.format, options.frameSize);

        long start = System.currentTimeMillis();
        JobId jobId = hcc.startJob(job,
                options.runtimeProfiling ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.waitForCompletion(jobId);
        long end = System.currentTimeMillis();
        System.err.println(start + " " + end + " " + (end - start));
    }

    private static FileSplit[] parseFileSplits(String fileSplits) {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int idx = s.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("File split " + s + " not well formed");
            }
            File file = new File(s.substring(idx + 1));
            fSplits[i] = new UnmanagedFileSplit(s.substring(0, idx), file.getAbsolutePath());
            fileSize += file.length();
        }
        return fSplits;
    }

    private static JobSpecification createJob(FileSplit[] inSplits, FileSplit[] outSplits, String algo, int htSize,
            int frameLimit, String format, int frameSize) {
        JobSpecification spec = new JobSpecification(frameSize);

        IFileSplitProvider splitsProvider = new ConstantFileSplitProvider(inSplits);
        RecordDescriptor wordDesc =
                new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor wordScanner =
                new FileScanOperatorDescriptor(spec, splitsProvider, new WordTupleParserFactory(), wordDesc);
        createPartitionConstraint(spec, wordScanner, inSplits);

        RecordDescriptor groupResultDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });

        IOperatorDescriptor gBy;
        int[] keys = new int[] { 0 };
        if ("hash".equalsIgnoreCase(algo)) {
            gBy = new ExternalGroupOperatorDescriptor(spec, htSize, fileSize, keys, frameLimit,
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

            createPartitionConstraint(spec, gBy, outSplits);
            IConnectorDescriptor scanGroupConn = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
            spec.connect(scanGroupConn, wordScanner, 0, gBy, 0);
        } else {
            IBinaryComparatorFactory[] cfs =
                    new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE };
            IOperatorDescriptor sorter =
                    "memsort".equalsIgnoreCase(algo)
                            ? new InMemorySortOperatorDescriptor(spec, keys,
                                    new INormalizedKeyComputerFactory[] {
                                            new UTF8StringNormalizedKeyComputerFactory() },
                                    cfs, wordDesc)
                            : new ExternalSortOperatorDescriptor(spec, frameLimit, keys,
                                    new UTF8StringNormalizedKeyComputerFactory(), cfs, wordDesc);
            createPartitionConstraint(spec, sorter, outSplits);

            IConnectorDescriptor scanSortConn = new MToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                            PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
            spec.connect(scanSortConn, wordScanner, 0, sorter, 0);

            gBy = new PreclusteredGroupOperatorDescriptor(spec, keys,
                    new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                    new MultiFieldsAggregatorFactory(
                            new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                    groupResultDesc, false, -1);
            createPartitionConstraint(spec, gBy, outSplits);
            OneToOneConnectorDescriptor sortGroupConn = new OneToOneConnectorDescriptor(spec);
            spec.connect(sortGroupConn, sorter, 0, gBy, 0);
        }

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(outSplits);
        IOperatorDescriptor writer =
                "text".equalsIgnoreCase(format) ? new PlainFileWriterOperatorDescriptor(spec, outSplitProvider, ",")
                        : new FrameFileWriterOperatorDescriptor(spec, outSplitProvider);
        createPartitionConstraint(spec, writer, outSplits);

        IConnectorDescriptor gbyPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(gbyPrinterConn, gBy, 0, writer, 0);

        spec.addRoot(writer);
        return spec;
    }

    private static void createPartitionConstraint(JobSpecification spec, IOperatorDescriptor op, FileSplit[] splits) {
        String[] parts = new String[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            parts[i] = splits[i].getNodeName();
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, parts);
    }
}
