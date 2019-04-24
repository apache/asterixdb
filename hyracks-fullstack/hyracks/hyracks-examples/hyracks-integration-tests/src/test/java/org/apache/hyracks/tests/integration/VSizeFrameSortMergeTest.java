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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.junit.Test;

public class VSizeFrameSortMergeTest extends AbstractIntegrationTest {

    AtomicInteger aInteger = new AtomicInteger(0);
    FileSplit[] ordersSplits = new FileSplit[] {
            new ManagedFileSplit(NC1_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders-part1.tbl"),
            new ManagedFileSplit(NC2_ID, "data" + File.separator + "tpch0.001" + File.separator + "orders-part2.tbl") };
    IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(ordersSplits);
    RecordDescriptor ordersDesc =
            new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    @Test
    public void sortNormalMergeTest() throws Exception {
        sortTask(1024, 4);
        sortTask(256, 4);
    }

    @Test
    public void sortLargeMergeTest() throws Exception {
        sortTask(32, 128);
        sortTask(16, 256);
        sortTask(16, 10240);
    }

    public void sortTask(int frameSize, int frameLimit) throws Exception {
        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, '|'),
                ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID, NC2_ID);

        spec.setFrameSize(frameSize);
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec,
                frameLimit, new int[] { 1, 0 }, new IBinaryComparatorFactory[] {
                        UTF8StringBinaryComparatorFactory.INSTANCE, UTF8StringBinaryComparatorFactory.INSTANCE },
                ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID, NC2_ID);

        String path = getClass().getSimpleName() + aInteger.getAndIncrement() + ".tmp";

        IFileSplitProvider outputSplitProvider =
                new ConstantFileSplitProvider(new FileSplit[] { new ManagedFileSplit(NC1_ID, path) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outputSplitProvider, "|");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), ordScanner, 0, sorter, 0);

        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, new FieldHashPartitionComputerFactory(
                new int[] { 1, 0 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY),
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }),
                new int[] { 1, 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE,
                        UTF8StringBinaryComparatorFactory.INSTANCE },
                new UTF8StringNormalizedKeyComputerFactory()), sorter, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.out.println("Result write into :" + path);
    }
}
