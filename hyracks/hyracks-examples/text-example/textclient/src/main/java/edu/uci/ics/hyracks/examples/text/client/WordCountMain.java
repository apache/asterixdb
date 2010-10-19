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
package edu.uci.ics.hyracks.examples.text.client;

import java.io.File;
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
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.CountAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IFieldValueResultingAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.FrameFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.PreclusteredGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;
import edu.uci.ics.hyracks.examples.text.WordTupleParserFactory;

public class WordCountMain {
    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-infile-splits", usage = "Comma separated list of file-splits for the input. A file-split is <node-name>:<path>", required = true)
        public String inFileSplits;

        @Option(name = "-outfile-splits", usage = "Comma separated list of file-splits for the output", required = true)
        public String outFileSplits;

        @Option(name = "-algo", usage = "Use Hash based grouping", required = true)
        public String algo;

        @Option(name = "-hashtable-size", usage = "Hash table size (default: 8191)", required = false)
        public int htSize = 8191;

        @Option(name = "-sortbuffer-size", usage = "Sort buffer size in frames (default: 32768)", required = false)
        public int sbSize = 32768;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job = createJob(parseFileSplits(options.inFileSplits), parseFileSplits(options.outFileSplits),
                options.algo, options.htSize, options.sbSize);

        long start = System.currentTimeMillis();
        UUID jobId = hcc.createJob(options.app, job);
        hcc.start(jobId);
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
            fSplits[i] = new FileSplit(s.substring(0, idx), new File(s.substring(idx + 1)));
        }
        return fSplits;
    }

    private static JobSpecification createJob(FileSplit[] inSplits, FileSplit[] outSplits, String algo, int htSize,
            int sbSize) {
        JobSpecification spec = new JobSpecification();

        IFileSplitProvider splitsProvider = new ConstantFileSplitProvider(inSplits);
        RecordDescriptor wordDesc = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor wordScanner = new FileScanOperatorDescriptor(spec, splitsProvider,
                new WordTupleParserFactory(), wordDesc);
        wordScanner.setPartitionConstraint(createPartitionConstraint(inSplits));

        RecordDescriptor groupResultDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        IOperatorDescriptor gBy;
        int[] keys = new int[] { 0 };
        if ("hash".equalsIgnoreCase(algo)) {
            gBy = new HashGroupOperatorDescriptor(spec, keys, new FieldHashPartitionComputerFactory(keys,
                    new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                    new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                    new MultiAggregatorFactory(
                            new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory() }),
                    groupResultDesc, htSize);
            gBy.setPartitionConstraint(createPartitionConstraint(outSplits));
            IConnectorDescriptor scanGroupConn = new MToNHashPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys,
                            new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
            spec.connect(scanGroupConn, wordScanner, 0, gBy, 0);
        } else {
            IBinaryComparatorFactory[] cfs = new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE };
            IOperatorDescriptor sorter = "memsort".equalsIgnoreCase(algo) ? new InMemorySortOperatorDescriptor(spec,
                    keys, new UTF8StringNormalizedKeyComputerFactory(), cfs, wordDesc)
                    : new ExternalSortOperatorDescriptor(spec, sbSize, keys, cfs, wordDesc);
            sorter.setPartitionConstraint(createPartitionConstraint(outSplits));

            IConnectorDescriptor scanSortConn = new MToNHashPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keys,
                            new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
            spec.connect(scanSortConn, wordScanner, 0, sorter, 0);

            gBy = new PreclusteredGroupOperatorDescriptor(spec, keys,
                    new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                    new MultiAggregatorFactory(
                            new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory() }),
                    groupResultDesc);
            gBy.setPartitionConstraint(createPartitionConstraint(outSplits));
            OneToOneConnectorDescriptor sortGroupConn = new OneToOneConnectorDescriptor(spec);
            spec.connect(sortGroupConn, sorter, 0, gBy, 0);
        }

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(outSplits);
        FrameFileWriterOperatorDescriptor writer = new FrameFileWriterOperatorDescriptor(spec, outSplitProvider);
        writer.setPartitionConstraint(createPartitionConstraint(outSplits));

        IConnectorDescriptor gbyPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(gbyPrinterConn, gBy, 0, writer, 0);

        spec.addRoot(writer);
        return spec;
    }

    private static PartitionConstraint createPartitionConstraint(FileSplit[] splits) {
        LocationConstraint[] lConstraints = new LocationConstraint[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            lConstraints[i] = new AbsoluteLocationConstraint(splits[i].getNodeName());
        }
        return new ExplicitPartitionConstraint(lConstraints);
    }
}