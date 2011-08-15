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

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksRMIConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.IntegerBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.CountAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.CountAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IFieldValueResultingAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IntSumAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.FrameFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableGroupingTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.PreclusteredGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;

/**
 * The application client for the performance tests of the external hash group
 * operator.
 */
public class ExternalGroupClient {
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

        @Option(name = "-hashtable-size", usage = "Hash table size (default: 8191)", required = false)
        public int htSize = 8191;

        @Option(name = "-frames-limit", usage = "Frame size (default: 32768)", required = false)
        public int framesLimit = 32768;

        @Option(name = "-sortbuffer-size", usage = "Sort buffer size in frames (default: 512)", required = false)
        public int sbSize = 512;

        @Option(name = "-sort-output", usage = "Whether to sort the output (default: true)", required = false)
        public boolean sortOutput = false;

        @Option(name = "-out-plain", usage = "Whether to output plain text (default: true)", required = false)
        public boolean outPlain = true;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);

        IHyracksClientConnection hcc = new HyracksRMIConnection(options.host, options.port);

        JobSpecification job;

        for (int i = 0; i < 3; i++) {
            long start = System.currentTimeMillis();
            job = createJob(parseFileSplits(options.inFileSplits), parseFileSplits(options.outFileSplits, i % 2),
                    options.htSize, options.sbSize, options.framesLimit, options.sortOutput, i % 3, options.outPlain);

            System.out.print(i + "\t" + (System.currentTimeMillis() - start));
            start = System.currentTimeMillis();
            JobId jobId = hcc.createJob(options.app, job);
            hcc.start(jobId);
            hcc.waitForCompletion(jobId);
            System.out.println("\t" + (System.currentTimeMillis() - start));
        }
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
            fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(new File(s.substring(idx + 1))));
        }
        return fSplits;
    }

    private static FileSplit[] parseFileSplits(String fileSplits, int count) {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int idx = s.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("File split " + s + " not well formed");
            }
            fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(new File(s.substring(idx + 1) + "_"
                    + count)));
        }
        return fSplits;
    }

    private static JobSpecification createJob(FileSplit[] inSplits, FileSplit[] outSplits, int htSize, int sbSize,
            int framesLimit, boolean sortOutput, int alg, boolean outPlain) {
        JobSpecification spec = new JobSpecification();
        IFileSplitProvider splitsProvider = new ConstantFileSplitProvider(inSplits);

        RecordDescriptor inDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor fileScanner = new FileScanOperatorDescriptor(spec, splitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, }, '|'), inDesc);

        createPartitionConstraint(spec, fileScanner, inSplits);

        // Output: each unique string with an integer count
        RecordDescriptor outDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE,
                // IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE });

        // Specify the grouping key, which will be the string extracted during
        // the scan.
        int[] keys = new int[] { 0,
        // 1
        };

        AbstractOperatorDescriptor grouper;

        switch (alg) {
            case 0: // External hash group
                grouper = new ExternalGroupOperatorDescriptor(
                        spec,
                        keys,
                        framesLimit,
                        new IBinaryComparatorFactory[] {
                        // IntegerBinaryComparatorFactory.INSTANCE,
                        IntegerBinaryComparatorFactory.INSTANCE },
                        new IntegerNormalizedKeyComputerFactory(),
                        new MultiAggregatorDescriptorFactory(
                                new IAggregatorDescriptorFactory[] { new CountAggregatorDescriptorFactory() }),
                        new MultiAggregatorDescriptorFactory(
                                new IAggregatorDescriptorFactory[] { new IntSumAggregatorDescriptorFactory(keys.length) }),
                        outDesc, new HashSpillableGroupingTableFactory(new FieldHashPartitionComputerFactory(keys,
                                new IBinaryHashFunctionFactory[] {
                                // IntegerBinaryHashFunctionFactory.INSTANCE,
                                IntegerBinaryHashFunctionFactory.INSTANCE }), htSize), false);

                createPartitionConstraint(spec, grouper, outSplits);

                // Connect scanner with the grouper
                IConnectorDescriptor scanGroupConn = new MToNPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                        // IntegerBinaryHashFunctionFactory.INSTANCE,
                                IntegerBinaryHashFunctionFactory.INSTANCE }));
                spec.connect(scanGroupConn, fileScanner, 0, grouper, 0);
                break;
            case 1: // External sort + pre-cluster
                ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, framesLimit, keys,
                        new IBinaryComparatorFactory[] {
                        // IntegerBinaryComparatorFactory.INSTANCE,
                        IntegerBinaryComparatorFactory.INSTANCE }, inDesc);
                createPartitionConstraint(spec, sorter, inSplits);

                // Connect scan operator with the sorter
                IConnectorDescriptor scanSortConn = new MToNPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                        // IntegerBinaryHashFunctionFactory.INSTANCE,
                                IntegerBinaryHashFunctionFactory.INSTANCE }));
                spec.connect(scanSortConn, fileScanner, 0, sorter, 0);

                grouper = new PreclusteredGroupOperatorDescriptor(spec, keys, new IBinaryComparatorFactory[] {
                // IntegerBinaryComparatorFactory.INSTANCE,
                        IntegerBinaryComparatorFactory.INSTANCE }, new MultiAggregatorFactory(
                                new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory() }), outDesc);

                createPartitionConstraint(spec, grouper, outSplits);

                // Connect sorter with the pre-cluster
                OneToOneConnectorDescriptor sortGroupConn = new OneToOneConnectorDescriptor(spec);
                spec.connect(sortGroupConn, sorter, 0, grouper, 0);
                break;
            case 2: // In-memory hash group
                grouper = new HashGroupOperatorDescriptor(spec, keys, new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] {
                        // IntegerBinaryHashFunctionFactory.INSTANCE,
                        IntegerBinaryHashFunctionFactory.INSTANCE }), new IBinaryComparatorFactory[] {
                // IntegerBinaryComparatorFactory.INSTANCE,
                        IntegerBinaryComparatorFactory.INSTANCE }, new MultiAggregatorFactory(
                                new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory() }), outDesc,
                        htSize);

                createPartitionConstraint(spec, grouper, outSplits);

                // Connect scanner with the grouper
                IConnectorDescriptor scanConn = new MToNPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                        // IntegerBinaryHashFunctionFactory.INSTANCE,
                                IntegerBinaryHashFunctionFactory.INSTANCE }));
                spec.connect(scanConn, fileScanner, 0, grouper, 0);
                break;
            default:
                grouper = new ExternalGroupOperatorDescriptor(
                        spec,
                        keys,
                        framesLimit,
                        new IBinaryComparatorFactory[] {
                        // IntegerBinaryComparatorFactory.INSTANCE,
                        IntegerBinaryComparatorFactory.INSTANCE },
                        new IntegerNormalizedKeyComputerFactory(),
                        new MultiAggregatorDescriptorFactory(
                                new IAggregatorDescriptorFactory[] { new CountAggregatorDescriptorFactory() }),
                        new MultiAggregatorDescriptorFactory(
                                new IAggregatorDescriptorFactory[] { new IntSumAggregatorDescriptorFactory(keys.length) }),
                        outDesc, new HashSpillableGroupingTableFactory(new FieldHashPartitionComputerFactory(keys,
                                new IBinaryHashFunctionFactory[] {
                                // IntegerBinaryHashFunctionFactory.INSTANCE,
                                IntegerBinaryHashFunctionFactory.INSTANCE }), htSize), false);

                createPartitionConstraint(spec, grouper, outSplits);

                // Connect scanner with the grouper
                IConnectorDescriptor scanGroupConnDef = new MToNPartitioningConnectorDescriptor(spec,
                        new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                        // IntegerBinaryHashFunctionFactory.INSTANCE,
                                IntegerBinaryHashFunctionFactory.INSTANCE }));
                spec.connect(scanGroupConnDef, fileScanner, 0, grouper, 0);
        }

        IFileSplitProvider outSplitProvider = new ConstantFileSplitProvider(outSplits);

        AbstractSingleActivityOperatorDescriptor writer;

        if (outPlain)
            writer = new PlainFileWriterOperatorDescriptor(spec, outSplitProvider, "|");
        else
            writer = new FrameFileWriterOperatorDescriptor(spec, outSplitProvider);

        createPartitionConstraint(spec, writer, outSplits);

        IConnectorDescriptor groupOutConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(groupOutConn, grouper, 0, writer, 0);

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