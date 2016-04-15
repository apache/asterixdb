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
package org.apache.hyracks.tests.integration;

import java.io.File;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.connectors.ConnectorPolicyFactory;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.api.dataflow.connectors.PipeliningConnectorPolicy;
import org.apache.hyracks.api.dataflow.connectors.SendSideMaterializedBlockingConnectorPolicy;
import org.apache.hyracks.api.dataflow.connectors.SendSideMaterializedPipeliningConnectorPolicy;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.FieldRangePartitionComputerFactory;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.LineFileWriteOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.HashSpillableTableFactory;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.AvgFieldGroupAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.AvgFieldMergeAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.MaterializingOperatorDescriptor;
import org.apache.hyracks.dataflow.std.parallel.HistogramAlgorithm;
import org.apache.hyracks.dataflow.std.parallel.base.FieldRangePartitionDelayComputerFactory;
import org.apache.hyracks.dataflow.std.parallel.base.HistogramConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.dataflow.std.parallel.histogram.AbstractSampleOperatorDescriptor;
import org.apache.hyracks.dataflow.std.parallel.histogram.MaterializingForwardOperatorDescriptor;
import org.apache.hyracks.dataflow.std.parallel.histogram.MaterializingSampleOperatorDescriptor;
import org.apache.hyracks.dataflow.std.parallel.histogram.MergeSampleOperatorDescriptor;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;

/**
 * @author michael
 */
public class SampleForwardTest extends AbstractIntegrationTest {
    private static final boolean DEBUG = false;
    private static final int balance_factor = 10;
    private static final int outputArity = 1;
    private static final int rangeMergeArity = 1;
    private static final int outputFiles = 2;
    private static final int outputRaws = 2;
    private static int[] sampleFields = new int[] { 2 };
    private static int[] normalFields = new int[] { 0 };
    /*private IBinaryComparatorFactory[] sampleCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(IntegerPointable.FACTORY) };
    private INormalizedKeyComputerFactory sampleKeyFactories = new IntegerNormalizedKeyComputerFactory();*/
    private IBinaryComparatorFactory[] sampleCmpFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
            .of(UTF8StringPointable.FACTORY) };
    private INormalizedKeyComputerFactory sampleKeyFactories = new UTF8StringNormalizedKeyComputerFactory();
    MultiFieldsAggregatorFactory sampleAggFactory = new MultiFieldsAggregatorFactory(
            new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, true) });

    //    @Test
    public void sampleForward_Sample() throws Exception {
        JobSpecification spec = new JobSpecification();
        File[] outputFile = new File[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            outputFile[i] = File.createTempFile("output-" + i + "-", null, new File("data"));
        }
        File[] outputRaw = new File[outputRaws];
        for (int i = 0; i < outputRaws; i++) {
            outputRaw[i] = File.createTempFile("raw-" + i + "-", null, new File("data"));
        }
        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };

        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        ExternalSortOperatorDescriptor sorterCust = new ExternalSortOperatorDescriptor(spec, 4, sampleFields,
                sampleCmpFactories, custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorterCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), custScanner, 0, sorterCust, 0);

        AbstractSampleOperatorDescriptor materSampleCust = new MaterializingSampleOperatorDescriptor(spec, 4,
                sampleFields, 2 * balance_factor, custDesc, sampleCmpFactories, null, 1, new boolean[] { true });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, materSampleCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorterCust, 0, materSampleCust, 0);

        byte[] byteRange = new byte[rangeMergeArity];
        int[] offRange = new int[rangeMergeArity];
        for (int i = 0; i < rangeMergeArity; i++) {
            byteRange[i] = Byte.parseByte(String.valueOf(i * (150 / rangeMergeArity + 1)));
            offRange[i] = i;
        }

        IRangeMap rangeMap = new RangeMap(rangeMergeArity, byteRange, offRange);

        ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(normalFields, sampleCmpFactories,
                rangeMap);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        FileSplit[] files = new FileSplit[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            files[i] = new FileSplit((0 == i % 2) ? NC1_ID : NC2_ID, new FileReference(outputFile[i]));
        }

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });
        IOperatorDescriptor forward = new MaterializingOperatorDescriptor(spec, outputRec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, forward, NC1_ID, NC2_ID);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, normalFields, sampleCmpFactories,
                sampleKeyFactories, false), materSampleCust, 0, forward, 0);

        IOperatorDescriptor printer = new LineFileWriteOperatorDescriptor(spec, files);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);
        spec.connect(new MToNReplicatingConnectorDescriptor(spec), forward, 0, printer, 0);

        ResultSetId rsRaw = new ResultSetId(2);
        spec.addResultSetId(rsRaw);
        FileSplit[] filesRaw = new FileSplit[outputRaws];
        for (int i = 0; i < outputRaws; i++) {
            filesRaw[i] = new FileSplit((0 == i % 2) ? NC1_ID : NC2_ID, new FileReference(outputRaw[i]));
        }
        IOperatorDescriptor printer1 = new LineFileWriteOperatorDescriptor(spec, filesRaw);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer1, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), materSampleCust, 1, printer1, 0);

        spec.addRoot(printer);
        spec.addRoot(printer1);
        runTest(spec);
    }

    //    @Test
    public void sampleForward_Merge() throws Exception {
        JobSpecification spec = new JobSpecification();
        File[] outputFile = new File[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            outputFile[i] = File.createTempFile("output-" + i + "-", null, new File("data"));
        }
        File[] outputRaw = new File[outputRaws];
        for (int i = 0; i < outputRaws; i++) {
            outputRaw[i] = File.createTempFile("raw-" + i + "-", null, new File("data"));
        }
        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        ExternalSortOperatorDescriptor sorterCust = new ExternalSortOperatorDescriptor(spec, 4, sampleFields,
                sampleCmpFactories, custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorterCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), custScanner, 0, sorterCust, 0);

        AbstractSampleOperatorDescriptor materSampleCust = new MaterializingSampleOperatorDescriptor(spec, 4,
                sampleFields, 2 * balance_factor, custDesc, sampleCmpFactories, null, 1, new boolean[] { true });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, materSampleCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorterCust, 0, materSampleCust, 0);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });

        byte[] byteRange = new byte[rangeMergeArity];
        int[] offRange = new int[rangeMergeArity];
        for (int i = 0; i < rangeMergeArity; i++) {
            byteRange[i] = Byte.parseByte(String.valueOf(i * (150 / rangeMergeArity + 1)));
            offRange[i] = i;
        }

        IRangeMap rangeMap = new RangeMap(rangeMergeArity, byteRange, offRange);

        ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(normalFields, sampleCmpFactories,
                rangeMap);

        IOperatorDescriptor mergeSampleCust = new MergeSampleOperatorDescriptor(spec, 4, normalFields, outputRec, 4,
                sampleKeyFactories, sampleCmpFactories, HistogramAlgorithm.ORDERED_HISTOGRAM, false);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, mergeSampleCust, NC1_ID, NC2_ID);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, normalFields, sampleCmpFactories,
                sampleKeyFactories, false), materSampleCust, 0, mergeSampleCust, 0);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        FileSplit[] files = new FileSplit[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            files[i] = new FileSplit((0 == i % 2) ? NC1_ID : NC2_ID, new FileReference(outputFile[i]));
        }

        IOperatorDescriptor printer = new LineFileWriteOperatorDescriptor(spec, files);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);
        spec.connect(new MToNReplicatingConnectorDescriptor(spec), mergeSampleCust, 0, printer, 0);

        ResultSetId rsRaw = new ResultSetId(2);
        spec.addResultSetId(rsRaw);
        FileSplit[] filesRaw = new FileSplit[outputRaws];
        for (int i = 0; i < outputRaws; i++) {
            filesRaw[i] = new FileSplit((0 == i % 2) ? NC1_ID : NC2_ID, new FileReference(outputRaw[i]));
        }
        IOperatorDescriptor printer1 = new LineFileWriteOperatorDescriptor(spec, filesRaw);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer1, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), materSampleCust, 1, printer1, 0);

        spec.addRoot(printer);
        spec.addRoot(printer1);
        runTest(spec);
    }

    //        @Test
    public void sampleForward_Total() throws Exception {
        JobSpecification spec = new JobSpecification();
        File[] outputFile = new File[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            outputFile[i] = File.createTempFile("output-" + i + "-", null, new File("data"));
        }
        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        ExternalSortOperatorDescriptor sorterCust = new ExternalSortOperatorDescriptor(spec, 4, sampleFields,
                sampleCmpFactories, custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorterCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), custScanner, 0, sorterCust, 0);

        AbstractSampleOperatorDescriptor materSampleCust = new MaterializingSampleOperatorDescriptor(spec, 4,
                sampleFields, 2 * balance_factor, custDesc, sampleCmpFactories, null, 1, new boolean[] { true });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, materSampleCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorterCust, 0, materSampleCust, 0);

        RecordDescriptor outputSamp = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        byte[] byteRange = new byte[rangeMergeArity];
        int[] offRange = new int[rangeMergeArity];
        for (int i = 0; i < rangeMergeArity; i++) {
            byteRange[i] = Byte.parseByte(String.valueOf(i * (150 / rangeMergeArity + 1)));
            offRange[i] = i;
        }

        IRangeMap rangeMap = new RangeMap(normalFields.length, byteRange, offRange);

        ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(normalFields, sampleCmpFactories,
                rangeMap);

        IOperatorDescriptor mergeSampleCust = new MergeSampleOperatorDescriptor(spec, 4, normalFields, outputSamp, 4,
                sampleKeyFactories, sampleCmpFactories, HistogramAlgorithm.ORDERED_HISTOGRAM, false);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, mergeSampleCust, NC1_ID, NC2_ID);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, normalFields, sampleCmpFactories,
                sampleKeyFactories, false), materSampleCust, 0, mergeSampleCust, 0);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());

        ITuplePartitionComputerFactory tpc = new FieldRangePartitionDelayComputerFactory(sampleFields,
                sampleCmpFactories);

        RecordDescriptor outputRec = custDesc;
        IOperatorDescriptor forward = new MaterializingForwardOperatorDescriptor(spec, 4, normalFields, outputSamp,
                outputRec, sampleCmpFactories);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, forward, NC1_ID, NC2_ID);
        spec.connect(new MToNReplicatingConnectorDescriptor(spec), mergeSampleCust, 0, forward, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materSampleCust, 1, forward, 1);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        FileSplit[] files = new FileSplit[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            files[i] = new FileSplit((0 == i % 2) ? NC1_ID : NC2_ID, new FileReference(outputFile[i]));
        }

        /*byteRange = new byte[32];
        offRange = new int[4];
        int current = 0;
        for (int i = 0; i < 4; i++) {
            offRange[i] = current;
            byte[] tick = ByteBuffer.allocate(4).putInt(i * (15000 / 4 + 1)).array();
            for (int j = 0; j < tick.length; j++) {
                byteRange[current + j] = tick[j];
            }
            current += tick.length;;
        }
        rangeMap = new RangeMap(normalFields.length, byteRange, offRange);
        tpcf = new FieldRangePartitionComputerFactory(normalFields, sampleCmpFactories, rangeMap);*/

        IOperatorDescriptor printer = new LineFileWriteOperatorDescriptor(spec, files);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, tpc, sampleFields, sampleCmpFactories,
                sampleKeyFactories, false), forward, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void sampleSort() throws Exception {
        JobSpecification spec = new JobSpecification();
        File[] outputFile = new File[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            outputFile[i] = File.createTempFile("output-" + i + "-", null, new File("data"));
        }
        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        AbstractSampleOperatorDescriptor materSampleCust = new MaterializingSampleOperatorDescriptor(spec, 4,
                sampleFields, 2 * balance_factor, custDesc, sampleCmpFactories, null, 1, new boolean[] { true });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, materSampleCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), custScanner, 0, materSampleCust, 0);

        RecordDescriptor outputSamp = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        byte[] byteRange = new byte[rangeMergeArity];
        int[] offRange = new int[rangeMergeArity];
        for (int i = 0; i < rangeMergeArity; i++) {
            byteRange[i] = Byte.parseByte(String.valueOf(i * (150 / rangeMergeArity + 1)));
            offRange[i] = i;
        }

        IRangeMap rangeMap = new RangeMap(normalFields.length, byteRange, offRange);

        ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(normalFields, sampleCmpFactories,
                rangeMap);

        IOperatorDescriptor mergeSampleCust = new MergeSampleOperatorDescriptor(spec, 4, normalFields, outputSamp, 4,
                sampleKeyFactories, sampleCmpFactories, HistogramAlgorithm.ORDERED_HISTOGRAM, false);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, mergeSampleCust, NC1_ID, NC2_ID);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, normalFields, sampleCmpFactories,
                sampleKeyFactories, false), materSampleCust, 0, mergeSampleCust, 0);

        ITuplePartitionComputerFactory tpc = new FieldRangePartitionDelayComputerFactory(sampleFields,
                sampleCmpFactories);

        RecordDescriptor outputRec = custDesc;
        IOperatorDescriptor forward = new MaterializingForwardOperatorDescriptor(spec, 4, normalFields, outputSamp,
                outputRec, sampleCmpFactories);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, forward, NC1_ID, NC2_ID);
        spec.connect(new MToNReplicatingConnectorDescriptor(spec), mergeSampleCust, 0, forward, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materSampleCust, 1, forward, 1);

        ExternalSortOperatorDescriptor sorterCust = new ExternalSortOperatorDescriptor(spec, 4, sampleFields,
                sampleCmpFactories, custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorterCust, NC1_ID, NC2_ID);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, tpc), forward, 0, sorterCust, 0);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        FileSplit[] files = new FileSplit[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            files[i] = new FileSplit((0 == i % 2) ? NC1_ID : NC2_ID, new FileReference(outputFile[i]));
        }

        IOperatorDescriptor printer = new LineFileWriteOperatorDescriptor(spec, files);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorterCust, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    //    @Test
    public void sampleForward_Case1() throws Exception {
        JobSpecification spec = new JobSpecification();
        File[] outputFile = new File[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            outputFile[i] = File.createTempFile("output-" + i + "-", null, new File("data"));
        }
        File[] outputRaw = new File[outputRaws];
        for (int i = 0; i < outputRaws; i++) {
            outputRaw[i] = File.createTempFile("raw-" + i + "-", null, new File("data"));
        }
        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        //        FileSplit[] custSplits = new FileSplit[] {
        //                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/customer-part1.tbl"))),
        //                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/customer-part2.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new FileReference(new File("data/tpch0.001/orders-part1.tbl"))),
                new FileSplit(NC2_ID, new FileReference(new File("data/tpch0.001/orders-part2.tbl"))) };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer() });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                new UTF8StringSerializerDeserializer() });

        /*FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);*/

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        /*ExternalSortOperatorDescriptor sorterOrd = new ExternalSortOperatorDescriptor(spec, 4, new int[] { 1 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorterOrd, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorterOrd, 0);*/

        ExternalSortOperatorDescriptor sorterCust = new ExternalSortOperatorDescriptor(spec, 4, sampleFields,
                sampleCmpFactories, custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorterCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), custScanner, 0, sorterCust, 0);

        /*MaterializingOperatorDescriptor materOrd = new MaterializingOperatorDescriptor(spec, ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, materOrd, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorterOrd, 0, materOrd, 0);*/

        AbstractSampleOperatorDescriptor materSampleCust = new MaterializingSampleOperatorDescriptor(spec, 4,
                sampleFields, 2 * balance_factor, custDesc, sampleCmpFactories, null, 1, new boolean[] { true });
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, materSampleCust, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorterCust, 0, materSampleCust, 0);

        /*ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(new int[] { 0 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) }, rangeMap);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, tpcf), sorterCust, 0, materSampleCust, 0);*/

        //        spec.connect(
        //                new MToNPartitioningConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(new int[] { 1, 0 },
        //                        new IBinaryHashFunctionFactory[] {
        //                                PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
        //                                PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) })), sorterCust, 0,
        //                materSampleCust, 0);

        /*spec.connect(
                new MToNPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(new int[] {
                        1, 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }), new int[] { 1, 0 },
                        new IBinaryComparatorFactory[] {
                                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                        new UTF8StringNormalizedKeyComputerFactory()), sorterCust, 0, materSampleCust, 0);*/

        //        MaterializingOperatorDescriptor materCust = new MaterializingOperatorDescriptor(spec, custDesc);
        //        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, materCust, NC1_ID, NC2_ID);
        //        spec.connect(new OneToOneConnectorDescriptor(spec), sorterCust, 0, materCust, 0);

        //
        //        ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(offRange,
        //                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
        //                rangeMap);
        //        spec.connect(new MToNPartitioningConnectorDescriptor(spec, tpcf), sorterCust, 0, materCust, 0);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });
        //        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
        //                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        /*ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(spec, sampleFields, 4,
                sampleCmpFactories, sampleKeyFactories, sampleAggFactory, sampleAggFactory, outputRec, null, false);*/

        //        PreclusteredGroupOperatorDescriptor grouper = new PreclusteredGroupOperatorDescriptor(spec, sampleFields,
        //                sampleCmpFactories, sampleAggFactory, outputRec);
        //        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC1_ID, NC2_ID);
        //        spec.connect(new OneToOneConnectorDescriptor(spec), sorterCust, 0, grouper, 0);

        byte[] byteRange = new byte[rangeMergeArity];
        int[] offRange = new int[rangeMergeArity];
        for (int i = 0; i < rangeMergeArity; i++) {
            byteRange[i] = Byte.parseByte(String.valueOf(i * (150 / rangeMergeArity + 1)));
            offRange[i] = i;
        }

        IRangeMap rangeMap = new RangeMap(rangeMergeArity, byteRange, offRange);

        ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(normalFields, sampleCmpFactories,
                rangeMap);
        //        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, normalFields, sampleCmpFactories,
        //                sampleKeyFactories, false), materSampleCust, 0, grouper, 0);

        //        spec.setConnectorPolicyAssignmentPolicy(new SampleConnectorPolicyAssignmentPolicy());
        //        spec.setUseConnectorPolicyForScheduling(true);
        //        spec.setConnectorPolicyAssignmentPolicy(IConnectorPolicyAssignmentPolicy); // IConnectorPolicy
        //        spec.connect(new MToNPartitioningConnectorDescriptor(spec, tpcf), materSampleCust, 0, grouper, 0);

        //        spec.connect(new MToNReplicatingConnectorDescriptor(spec), grouper, 0, materSampleCust, 1);

        IOperatorDescriptor mergeSampleCust = new MergeSampleOperatorDescriptor(spec, 4, normalFields, outputRec, 4,
                sampleKeyFactories, sampleCmpFactories, HistogramAlgorithm.ORDERED_HISTOGRAM, false);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, mergeSampleCust, NC1_ID, NC2_ID);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, tpcf, normalFields, sampleCmpFactories,
                sampleKeyFactories, false), materSampleCust, 0, mergeSampleCust, 0);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        FileSplit[] files = new FileSplit[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            files[i] = new FileSplit((0 == i % 2) ? NC1_ID : NC2_ID, new FileReference(outputFile[i]));
        }

        IOperatorDescriptor printer = new LineFileWriteOperatorDescriptor(spec, files);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);
        spec.connect(new MToNReplicatingConnectorDescriptor(spec), mergeSampleCust, 0, printer, 0);
        //        spec.connect(new OneToOneConnectorDescriptor(spec), materSampleCust, 0, printer, 0);

        ResultSetId rsRaw = new ResultSetId(2);
        spec.addResultSetId(rsRaw);
        FileSplit[] filesRaw = new FileSplit[outputRaws];
        for (int i = 0; i < outputRaws; i++) {
            filesRaw[i] = new FileSplit((0 == i % 2) ? NC1_ID : NC2_ID, new FileReference(outputRaw[i]));
        }

        IOperatorDescriptor printer1 = new LineFileWriteOperatorDescriptor(spec, filesRaw);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer1, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), materSampleCust, 1, printer1, 0);

        //        spec.connect(new OneToOneConnectorDescriptor(spec), materSampleCust, 1, printer, 0);
        /*byte[] byteRange = new byte[outputFiles];
        int[] offRange = new int[outputFiles];
        for (int i = 0; i < outputFiles; i++) {
            byteRange[i] = Byte.parseByte(String.valueOf(i * 40));
            offRange[i] = i;
        }
        IRangeMap rangeMap = new RangeMap(1, byteRange, offRange);
        ITuplePartitionComputerFactory tpcf = new FieldRangePartitionComputerFactory(new int[] { 0 },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                rangeMap);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, tpcf), materCust, 0, printer, 0);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);*/

        /*IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, false, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID, NC2_ID);
        spec.connect(new OneToOneConnectorDescriptor(spec), materSampleCust, 0, printer, 0);*/

        spec.addRoot(printer);
        spec.addRoot(printer1);
        runTest(spec);
    }
}
