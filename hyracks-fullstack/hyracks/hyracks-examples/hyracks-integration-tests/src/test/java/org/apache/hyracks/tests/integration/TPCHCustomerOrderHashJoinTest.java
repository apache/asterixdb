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
package org.apache.hyracks.tests.integration;

import java.io.File;
import java.util.Arrays;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryHashFunctionFamily;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNBroadcastConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.InMemoryHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.join.JoinComparatorFactory;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoinOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.MaterializingOperatorDescriptor;
import org.apache.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.tests.util.NoopMissingWriterFactory;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;
import org.junit.Test;

public class TPCHCustomerOrderHashJoinTest extends AbstractIntegrationTest {

    /*
     * TPCH Customer table: CREATE TABLE CUSTOMER ( C_CUSTKEY INTEGER NOT NULL,
     * C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY
     * INTEGER NOT NULL, C_PHONE CHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT
     * NULL, C_MKTSEGMENT CHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL );
     * TPCH Orders table: CREATE TABLE ORDERS ( O_ORDERKEY INTEGER NOT NULL,
     * O_CUSTKEY INTEGER NOT NULL, O_ORDERSTATUS CHAR(1) NOT NULL, O_TOTALPRICE
     * DECIMAL(15,2) NOT NULL, O_ORDERDATE DATE NOT NULL, O_ORDERPRIORITY
     * CHAR(15) NOT NULL, O_CLERK CHAR(15) NOT NULL, O_SHIPPRIORITY INTEGER NOT
     * NULL, O_COMMENT VARCHAR(79) NOT NULL );
     */

    private static boolean DEBUG = false;

    static RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    static RecordDescriptor ordersDesc =
            new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    static RecordDescriptor custOrderJoinDesc =
            new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    static IValueParserFactory[] custValueParserFactories = new IValueParserFactory[custDesc.getFieldCount()];
    static IValueParserFactory[] orderValueParserFactories = new IValueParserFactory[ordersDesc.getFieldCount()];

    static {
        Arrays.fill(custValueParserFactories, UTF8StringParserFactory.INSTANCE);
        Arrays.fill(orderValueParserFactories, UTF8StringParserFactory.INSTANCE);
    }

    @Test
    public void customerOrderCIDJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID, "data" + File.separator + "tpch0.001" + File.separator + "customer.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC2_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), custOrderJoinDesc, 128,
                null, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID, "data" + File.separator + "tpch0.001" + File.separator + "customer.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC2_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 32, 20, 1.2, new int[] { 1 }, new int[] { 0 },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        custOrderJoinDesc, new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0),
                        new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1), null, false, null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDInMemoryHashLeftOuterJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID, "data" + File.separator + "tpch0.001" + File.separator + "customer.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC2_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        IMissingWriterFactory[] nonMatchWriterFactories = new IMissingWriterFactory[ordersDesc.getFieldCount()];
        for (int j = 0; j < nonMatchWriterFactories.length; j++) {
            nonMatchWriterFactories[j] = NoopMissingWriterFactory.INSTANCE;
        }

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1), null, custOrderJoinDesc,
                true, nonMatchWriterFactories, 128, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashLeftOuterJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID, "data" + File.separator + "tpch0.001" + File.separator + "customer.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC2_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        IMissingWriterFactory[] nonMatchWriterFactories = new IMissingWriterFactory[ordersDesc.getFieldCount()];
        for (int j = 0; j < nonMatchWriterFactories.length; j++) {
            nonMatchWriterFactories[j] = NoopMissingWriterFactory.INSTANCE;
        }

        OptimizedHybridHashJoinOperatorDescriptor join =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 32, 20, 1.2, new int[] { 0 }, new int[] { 1 },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        custOrderJoinDesc, new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1),
                        new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), null, true,
                        nonMatchWriterFactories);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDJoinMulti() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "customer-part1.tbl"),
                new ManagedFileSplit(NC2_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "customer-part2.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "orders-part1.tbl"),
                new ManagedFileSplit(NC2_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "orders-part2.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), custOrderJoinDesc, 128,
                null, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashJoinMulti() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "customer-part1.tbl"),
                new ManagedFileSplit(NC2_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "customer-part2.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "orders-part1.tbl"),
                new ManagedFileSplit(NC2_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "orders-part2.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        OptimizedHybridHashJoinOperatorDescriptor join =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 5, 20, 1.2, new int[] { 1 }, new int[] { 0 },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE },
                        custOrderJoinDesc, new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0),
                        new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1), null, false, null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDJoinAutoExpand() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "customer-part1.tbl"),
                new ManagedFileSplit(NC2_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "customer-part2.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "orders-part1.tbl"),
                new ManagedFileSplit(NC2_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "orders-part2.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), custOrderJoinDesc, 128,
                null, 128);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, join, 2);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDJoinMultiMaterialized() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "customer-part1.tbl"),
                new ManagedFileSplit(NC2_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "customer-part2.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC1_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "orders-part1.tbl"),
                new ManagedFileSplit(NC2_ID,
                        "data" + File.separator + "tpch0.001" + File.separator + "orders-part2.tbl") };
        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID, NC2_ID);

        MaterializingOperatorDescriptor ordMat = new MaterializingOperatorDescriptor(spec, ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordMat, NC1_ID, NC2_ID);

        MaterializingOperatorDescriptor custMat = new MaterializingOperatorDescriptor(spec, custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custMat, NC1_ID, NC2_ID);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 1 },
                new int[] { 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) },
                new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), custOrderJoinDesc, 128,
                null, 128);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID, NC2_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor ordPartConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(ordPartConn, ordScanner, 0, ordMat, 0);

        IConnectorDescriptor custPartConn = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(custPartConn, custScanner, 0, custMat, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordMat, 0, join, 0);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custMat, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDHybridHashJoin_Case1() throws Exception {
        JobSpecification spec = new JobSpecification();
        FileSplit[] custSplits = new FileSplit[] { new ManagedFileSplit(NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "customer4.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC2_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders4.tbl") };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 243, 1.2, new int[] { 0 }, new int[] { 1 },
                        new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                        custOrderJoinDesc, new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1),
                        new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        String path = getClass().getName() + File.separator + "case1";
        IOperatorDescriptor printer = getPrinter(spec, path);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + path);
    }

    @Test
    public void customerOrderCIDHybridHashJoin_Case2() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new ManagedFileSplit(NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "customer3.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC2_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders4.tbl") };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 15, 122, 1.2, new int[] { 0 }, new int[] { 1 },
                        new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                        custOrderJoinDesc, new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1),
                        new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        String path = getClass().getName() + File.separator + "case2";
        IOperatorDescriptor printer = getPrinter(spec, path);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + path);
    }

    @Test
    public void customerOrderCIDHybridHashJoin_Case3() throws Exception {

        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new ManagedFileSplit(NC1_ID,
                "data" + File.separator + "tpch0.001" + File.separator + "customer3.tbl") };
        IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);

        FileSplit[] ordersSplits = new FileSplit[] {
                new ManagedFileSplit(NC2_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders1.tbl") };

        IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(orderValueParserFactories, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC2_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(custValueParserFactories, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        OptimizedHybridHashJoinOperatorDescriptor join =
                new OptimizedHybridHashJoinOperatorDescriptor(spec, 6, 122, 1.2, new int[] { 0 }, new int[] { 1 },
                        new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                        new IBinaryHashFunctionFamily[] { UTF8StringBinaryHashFunctionFamily.INSTANCE },
                        custOrderJoinDesc, new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 0, 1),
                        new JoinComparatorFactory(UTF8StringBinaryComparatorFactory.INSTANCE, 1, 0), null);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        String path = getClass().getName() + File.separator + "case3";
        IOperatorDescriptor printer = getPrinter(spec, path);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 0);

        IConnectorDescriptor ordJoinConn = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("output to " + path);
    }

    private IOperatorDescriptor getPrinter(JobSpecification spec, String path) {
        IFileSplitProvider outputSplitProvider =
                new ConstantFileSplitProvider(new FileSplit[] { new ManagedFileSplit(NC1_ID, path) });

        return DEBUG ? new PlainFileWriterOperatorDescriptor(spec, outputSplitProvider, "|")
                : new NullSinkOperatorDescriptor(spec);
    }
}
