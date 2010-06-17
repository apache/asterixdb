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
package edu.uci.ics.hyracks.tests.integration;

import java.io.File;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.AbsoluteLocationConstraint;
import edu.uci.ics.hyracks.api.constraints.LocationConstraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraint;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.coreops.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.coreops.MToNHashPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.coreops.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.coreops.MaterializingOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.coreops.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.data.StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.coreops.data.StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.coreops.data.StringSerializerDeserializer;
import edu.uci.ics.hyracks.coreops.file.CSVFileScanOperatorDescriptor;
import edu.uci.ics.hyracks.coreops.file.FileSplit;
import edu.uci.ics.hyracks.coreops.join.InMemoryHashJoinOperatorDescriptor;

public class TPCHCustomerOrderHashJoinTest extends AbstractIntegrationTest {
    /*
     * TPCH Customer table:
     * CREATE TABLE CUSTOMER (
     * C_CUSTKEY INTEGER NOT NULL,
     * C_NAME VARCHAR(25) NOT NULL,
     * C_ADDRESS VARCHAR(40) NOT NULL,
     * C_NATIONKEY INTEGER NOT NULL,
     * C_PHONE CHAR(15) NOT NULL,
     * C_ACCTBAL DECIMAL(15,2) NOT NULL,
     * C_MKTSEGMENT CHAR(10) NOT NULL,
     * C_COMMENT VARCHAR(117) NOT NULL
     * );
     * TPCH Orders table:
     * CREATE TABLE ORDERS (
     * O_ORDERKEY INTEGER NOT NULL,
     * O_CUSTKEY INTEGER NOT NULL,
     * O_ORDERSTATUS CHAR(1) NOT NULL,
     * O_TOTALPRICE DECIMAL(15,2) NOT NULL,
     * O_ORDERDATE DATE NOT NULL,
     * O_ORDERPRIORITY CHAR(15) NOT NULL,
     * O_CLERK CHAR(15) NOT NULL,
     * O_SHIPPRIORITY INTEGER NOT NULL,
     * O_COMMENT VARCHAR(79) NOT NULL
     * );
     */

    @Test
    public void customerOrderCIDJoin() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new File("data/tpch0.001/customer.tbl")) };
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC2_ID, new File("data/tpch0.001/orders.tbl")) };
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE });

        CSVFileScanOperatorDescriptor ordScanner = new CSVFileScanOperatorDescriptor(spec, ordersSplits, ordersDesc,
                '|', "'\"");
        PartitionConstraint ordersPartitionConstraint = new PartitionConstraint(
                new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        ordScanner.setPartitionConstraint(ordersPartitionConstraint);

        CSVFileScanOperatorDescriptor custScanner = new CSVFileScanOperatorDescriptor(spec, custSplits, custDesc, '|',
                "'\"");
        PartitionConstraint custPartitionConstraint = new PartitionConstraint(
                new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        custScanner.setPartitionConstraint(custPartitionConstraint);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 1 },
                new int[] { 0 }, new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE },
                new IBinaryComparatorFactory[] { StringBinaryComparatorFactory.INSTANCE }, custOrderJoinDesc, 128);
        PartitionConstraint joinPartitionConstraint = new PartitionConstraint(
                new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        join.setPartitionConstraint(joinPartitionConstraint);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerPartitionConstraint = new PartitionConstraint(
                new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        printer.setPartitionConstraint(printerPartitionConstraint);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDJoinMulti() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new File("data/tpch0.001/customer-part1.tbl")),
                new FileSplit(NC2_ID, new File("data/tpch0.001/customer-part2.tbl")) };
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new File("data/tpch0.001/orders-part1.tbl")),
                new FileSplit(NC2_ID, new File("data/tpch0.001/orders-part2.tbl")) };
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE });

        CSVFileScanOperatorDescriptor ordScanner = new CSVFileScanOperatorDescriptor(spec, ordersSplits, ordersDesc,
                '|', "'\"");
        PartitionConstraint ordersPartitionConstraint = new PartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID) });
        ordScanner.setPartitionConstraint(ordersPartitionConstraint);

        CSVFileScanOperatorDescriptor custScanner = new CSVFileScanOperatorDescriptor(spec, custSplits, custDesc, '|',
                "'\"");
        PartitionConstraint custPartitionConstraint = new PartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID) });
        custScanner.setPartitionConstraint(custPartitionConstraint);

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 1 },
                new int[] { 0 }, new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE },
                new IBinaryComparatorFactory[] { StringBinaryComparatorFactory.INSTANCE }, custOrderJoinDesc, 128);
        PartitionConstraint joinPartitionConstraint = new PartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID) });
        join.setPartitionConstraint(joinPartitionConstraint);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerPartitionConstraint = new PartitionConstraint(
                new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        printer.setPartitionConstraint(printerPartitionConstraint);

        IConnectorDescriptor ordJoinConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(ordJoinConn, ordScanner, 0, join, 0);

        IConnectorDescriptor custJoinConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(custJoinConn, custScanner, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void customerOrderCIDJoinMultiMaterialized() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] custSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new File("data/tpch0.001/customer-part1.tbl")),
                new FileSplit(NC2_ID, new File("data/tpch0.001/customer-part2.tbl")) };
        RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE });

        FileSplit[] ordersSplits = new FileSplit[] {
                new FileSplit(NC1_ID, new File("data/tpch0.001/orders-part1.tbl")),
                new FileSplit(NC2_ID, new File("data/tpch0.001/orders-part2.tbl")) };
        RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE });

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE, StringSerializerDeserializer.INSTANCE,
                StringSerializerDeserializer.INSTANCE });

        CSVFileScanOperatorDescriptor ordScanner = new CSVFileScanOperatorDescriptor(spec, ordersSplits, ordersDesc,
                '|', "'\"");
        PartitionConstraint ordersPartitionConstraint = new PartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID) });
        ordScanner.setPartitionConstraint(ordersPartitionConstraint);

        CSVFileScanOperatorDescriptor custScanner = new CSVFileScanOperatorDescriptor(spec, custSplits, custDesc, '|',
                "'\"");
        PartitionConstraint custPartitionConstraint = new PartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID) });
        custScanner.setPartitionConstraint(custPartitionConstraint);

        MaterializingOperatorDescriptor ordMat = new MaterializingOperatorDescriptor(spec, ordersDesc);
        ordMat.setPartitionConstraint(new PartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID) }));

        MaterializingOperatorDescriptor custMat = new MaterializingOperatorDescriptor(spec, custDesc);
        custMat.setPartitionConstraint(new PartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID) }));

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(spec, new int[] { 1 },
                new int[] { 0 }, new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE },
                new IBinaryComparatorFactory[] { StringBinaryComparatorFactory.INSTANCE }, custOrderJoinDesc, 128);
        PartitionConstraint joinPartitionConstraint = new PartitionConstraint(new LocationConstraint[] {
                new AbsoluteLocationConstraint(NC1_ID), new AbsoluteLocationConstraint(NC2_ID) });
        join.setPartitionConstraint(joinPartitionConstraint);

        PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
        PartitionConstraint printerPartitionConstraint = new PartitionConstraint(
                new LocationConstraint[] { new AbsoluteLocationConstraint(NC1_ID) });
        printer.setPartitionConstraint(printerPartitionConstraint);

        IConnectorDescriptor ordPartConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 },
                        new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(ordPartConn, ordScanner, 0, ordMat, 0);

        IConnectorDescriptor custPartConn = new MToNHashPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 },
                        new IBinaryHashFunctionFactory[] { StringBinaryHashFunctionFactory.INSTANCE }));
        spec.connect(custPartConn, custScanner, 0, custMat, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordMat, 0, join, 0);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custMat, 0, join, 1);

        IConnectorDescriptor joinPrinterConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}