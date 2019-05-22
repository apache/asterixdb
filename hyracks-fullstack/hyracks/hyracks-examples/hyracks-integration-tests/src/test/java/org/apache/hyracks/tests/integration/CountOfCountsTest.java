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

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
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
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.dataflow.std.sort.InMemorySortOperatorDescriptor;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;
import org.junit.Test;

public class CountOfCountsTest extends AbstractIntegrationTest {
    @Test
    public void countOfCountsSingleNC() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] splits = new FileSplit[] { new ManagedFileSplit(NC2_ID, "data" + File.separator + "words.txt") };
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(splits);
        RecordDescriptor desc =
                new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor csvScanner =
                new FileScanOperatorDescriptor(spec, splitProvider, new DelimitedDataTupleParserFactory(
                        new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','), desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        InMemorySortOperatorDescriptor sorter = new InMemorySortOperatorDescriptor(spec, new int[] { 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC2_ID);

        RecordDescriptor desc2 = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });
        PreclusteredGroupOperatorDescriptor group = new PreclusteredGroupOperatorDescriptor(spec, new int[] { 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                desc2, false, -1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group, NC2_ID);

        InMemorySortOperatorDescriptor sorter2 = new InMemorySortOperatorDescriptor(spec, new int[] { 1 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE }, desc2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter2, NC2_ID);

        RecordDescriptor desc3 = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        PreclusteredGroupOperatorDescriptor group2 = new PreclusteredGroupOperatorDescriptor(spec, new int[] { 1 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE },
                new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                desc3, false, -1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group2, NC2_ID);

        ResultSetId rsId = new ResultSetId(1);
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        spec.addResultSetId(rsId);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, sorter, 0);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, sorter, 0, group, 0);

        IConnectorDescriptor conn3 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn3, group, 0, sorter2, 0);

        IConnectorDescriptor conn4 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn4, sorter2, 0, group2, 0);

        IConnectorDescriptor conn5 = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(conn5, group2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void countOfCountsMultiNC() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] splits = new FileSplit[] { new ManagedFileSplit(NC2_ID, "data" + File.separator + "words.txt") };
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(splits);
        RecordDescriptor desc =
                new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor csvScanner =
                new FileScanOperatorDescriptor(spec, splitProvider, new DelimitedDataTupleParserFactory(
                        new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','), desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        InMemorySortOperatorDescriptor sorter = new InMemorySortOperatorDescriptor(spec, new int[] { 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID, NC2_ID, NC1_ID, NC2_ID);

        RecordDescriptor desc2 = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });
        PreclusteredGroupOperatorDescriptor group = new PreclusteredGroupOperatorDescriptor(spec, new int[] { 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                desc2, false, -1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group, NC1_ID, NC2_ID, NC1_ID, NC2_ID);

        InMemorySortOperatorDescriptor sorter2 = new InMemorySortOperatorDescriptor(spec, new int[] { 1 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE }, desc2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter2, NC1_ID, NC2_ID);

        RecordDescriptor desc3 = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        PreclusteredGroupOperatorDescriptor group2 = new PreclusteredGroupOperatorDescriptor(spec, new int[] { 1 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE },
                new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                desc3, false, -1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group2, NC1_ID, NC2_ID);

        ResultSetId rsId = new ResultSetId(1);
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        spec.addResultSetId(rsId);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, sorter, 0);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, sorter, 0, group, 0);

        IConnectorDescriptor conn3 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn3, group, 0, sorter2, 0);

        IConnectorDescriptor conn4 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn4, sorter2, 0, group2, 0);

        IConnectorDescriptor conn5 = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(conn5, group2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    @Test
    public void countOfCountsExternalSortMultiNC() throws Exception {
        JobSpecification spec = new JobSpecification();

        FileSplit[] splits = new FileSplit[] { new ManagedFileSplit(NC2_ID, "data" + File.separator + "words.txt") };
        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(splits);
        RecordDescriptor desc =
                new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor csvScanner =
                new FileScanOperatorDescriptor(spec, splitProvider, new DelimitedDataTupleParserFactory(
                        new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','), desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, NC2_ID);

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, 3, new int[] { 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE }, desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter, NC1_ID, NC2_ID, NC1_ID, NC2_ID);

        RecordDescriptor desc2 = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE });
        PreclusteredGroupOperatorDescriptor group = new PreclusteredGroupOperatorDescriptor(spec, new int[] { 0 },
                new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
                new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                desc2, false, -1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group, NC1_ID, NC2_ID, NC1_ID, NC2_ID);

        InMemorySortOperatorDescriptor sorter2 = new InMemorySortOperatorDescriptor(spec, new int[] { 1 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE }, desc2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, sorter2, NC1_ID, NC2_ID);

        RecordDescriptor desc3 = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        PreclusteredGroupOperatorDescriptor group2 = new PreclusteredGroupOperatorDescriptor(spec, new int[] { 1 },
                new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE },
                new MultiFieldsAggregatorFactory(
                        new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(true) }),
                desc3, false, -1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group2, NC1_ID, NC2_ID);

        ResultSetId rsId = new ResultSetId(1);
        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        spec.addResultSetId(rsId);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 0 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn1, csvScanner, 0, sorter, 0);

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, sorter, 0, group, 0);

        IConnectorDescriptor conn3 = new MToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(new int[] { 1 }, new IBinaryHashFunctionFactory[] {
                        PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY) }));
        spec.connect(conn3, group, 0, sorter2, 0);

        IConnectorDescriptor conn4 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn4, sorter2, 0, group2, 0);

        IConnectorDescriptor conn5 = new MToNBroadcastConnectorDescriptor(spec);
        spec.connect(conn5, group2, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }
}
