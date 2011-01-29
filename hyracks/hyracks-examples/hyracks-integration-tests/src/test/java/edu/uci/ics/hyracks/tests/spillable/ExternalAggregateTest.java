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
package edu.uci.ics.hyracks.tests.spillable;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.IntegerBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.IntegerBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.CountAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.IFieldValueResultingAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MinMaxAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.MultiAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.aggregators.SumAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalHashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

/**
 * Test cases for external hash group operator.
 */
public class ExternalAggregateTest extends AbstractIntegrationTest {

    /**
     * Test 01: aggregate (count) on single field, on a simple data set.
     * 
     * @throws Exception
     */
    @Test
    public void externalAggregateTestSingleFieldSimpleData() throws Exception {
        JobSpecification spec = new JobSpecification();

        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] {
                new FileSplit(NC2_ID, new FileReference(new File("data/wordcount.tsv"))),
                new FileSplit(NC1_ID, new FileReference(new File("data/wordcount.tsv"))) });

        // Input format: a string field as the key
        RecordDescriptor desc = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

        // Output format: a string field as the key, and an integer field as the
        // count
        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        // Data set format: word(string),count(int)
        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
                spec,
                splitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','),
                desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID, NC1_ID);

        int[] keys = new int[] { 0 };
        int tableSize = 8;

        ExternalHashGroupOperatorDescriptor grouper = new ExternalHashGroupOperatorDescriptor(
                spec, // Job conf
                keys, // Group key
                3, // Number of frames
                false, // Whether to sort the output
                // Hash partitioner
                new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                // Key comparator
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                // Aggregator factory
                new MultiAggregatorFactory(new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory() }),
                outputRec, // Output format
                tableSize // Size of the hashing table, which is used to control
        // the partition when hashing
        );

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    /**
     * Test 02: Control experiment using in-memory aggregator, on the same data
     * set of {@link #externalAggregateTest01()}
     * 
     * @throws Exception
     */
    @Test
    public void externalAggregateTestSingleFieldSimpleDataInMemControl() throws Exception {
        JobSpecification spec = new JobSpecification();

        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] {
                new FileSplit(NC2_ID, new FileReference(new File("data/wordcount.tsv"))),
                new FileSplit(NC1_ID, new FileReference(new File("data/wordcount.tsv"))) });

        RecordDescriptor desc = new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
                spec,
                splitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','),
                desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID, NC1_ID);

        int[] keys = new int[] { 0 };
        int tableSize = 8;

        HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(
                spec,
                keys,
                new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                new MultiAggregatorFactory(new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory() }),
                outputRec, tableSize);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC2_ID, NC1_ID);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(conn1, csvScanner, 0, grouper, 0);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    /**
     * Test 03: aggregates on multiple fields
     * 
     * @throws Exception
     */
    @Test
    public void externalAggregateTestMultiAggFields() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/lineitem.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE });

        int[] keys = new int[] { 0 };
        int tableSize = 8;

        ExternalHashGroupOperatorDescriptor grouper = new ExternalHashGroupOperatorDescriptor(spec, keys, 3, false,
                new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                new MultiAggregatorFactory(new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory(),
                        new SumAggregatorFactory(4), new MinMaxAggregatorFactory(true, 5) }), outputRec, tableSize);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC1_ID);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keys,
                        new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(conn1, ordScanner, 0, grouper, 0);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    /**
     * Test 05: aggregate on multiple key fields
     * 
     * @throws Exception
     */
    @Test
    public void externalAggregateTestMultiKeys() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/lineitem.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE });

        // Group on two fields
        int[] keys = new int[] { 0, 1 };
        int tableSize = 8;

        ExternalHashGroupOperatorDescriptor grouper = new ExternalHashGroupOperatorDescriptor(spec, keys, 3, false,
                new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                        UTF8StringBinaryHashFunctionFactory.INSTANCE, UTF8StringBinaryHashFunctionFactory.INSTANCE }),
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE,
                        UTF8StringBinaryComparatorFactory.INSTANCE }, new MultiAggregatorFactory(
                        new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory(),
                                new SumAggregatorFactory(4), new MinMaxAggregatorFactory(true, 5) }), outputRec,
                tableSize);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC1_ID);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                        UTF8StringBinaryHashFunctionFactory.INSTANCE, UTF8StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(conn1, ordScanner, 0, grouper, 0);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    /**
     * Test 06: tests on non-string key field
     * 
     * @throws Exception
     */
    @Test
    public void externalAggregateTestNonStringKey() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/lineitem.tbl"))) };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                FloatSerializerDeserializer.INSTANCE });

        // Group on two fields
        int[] keys = new int[] { 0, 1 };
        int tableSize = 8;

        ExternalHashGroupOperatorDescriptor grouper = new ExternalHashGroupOperatorDescriptor(spec, keys, 3000, true,
                new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                        IntegerBinaryHashFunctionFactory.INSTANCE, IntegerBinaryHashFunctionFactory.INSTANCE }),
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE,
                        IntegerBinaryComparatorFactory.INSTANCE }, new MultiAggregatorFactory(
                        new IFieldValueResultingAggregatorFactory[] { new CountAggregatorFactory(),
                                new SumAggregatorFactory(4), new MinMaxAggregatorFactory(true, 5) }), outputRec,
                tableSize);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, NC1_ID);

        IConnectorDescriptor conn1 = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keys, new IBinaryHashFunctionFactory[] {
                        IntegerBinaryHashFunctionFactory.INSTANCE, IntegerBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(conn1, ordScanner, 0, grouper, 0);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}