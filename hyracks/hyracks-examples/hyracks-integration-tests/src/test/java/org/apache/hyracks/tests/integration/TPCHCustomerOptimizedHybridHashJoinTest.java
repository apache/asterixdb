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
package edu.uci.ics.hyracks.tests.integration;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.UTF8StringBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.join.JoinComparatorFactory;
import edu.uci.ics.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;

public class TPCHCustomerOptimizedHybridHashJoinTest extends AbstractIntegrationTest {
    private static final boolean DEBUG = false;

    @Test
    public void customerOrderCIDHybridHashJoin_Case1() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer4.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders4.tbl"))) };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243,
                1.2, new int[] { 0 }, new int[] { 1 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, new JoinComparatorFactory(
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0), null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec)
                : new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashJoin_Case2() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer3.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders4.tbl"))) };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 122,
                1.2, new int[] { 0 }, new int[] { 1 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, new JoinComparatorFactory(
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0), null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec)
                : new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashJoin_Case3() throws Exception {

        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
                "data/tpch0.001/customer3.tbl"))) };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
                "data/tpch0.001/orders1.tbl"))) };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join = new OptimizedHybridHashJoinOperatorDescriptor(spec, 6, 122,
                1.2, new int[] { 0 }, new int[] { 1 },
                new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                custOrderJoinDesc, new JoinComparatorFactory(
                        PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 0, 1),
                new JoinComparatorFactory(PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY), 1, 0), null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec)
                : new NullSinkOperatorDescriptor(spec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

}
