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
import java.io.IOException;
import java.util.BitSet;

import org.junit.Test;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.GlobalHashingLocalityMap;
import org.apache.hyracks.dataflow.std.connectors.HashtableLocalityMap;
import org.apache.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;
import org.apache.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.FloatSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import org.apache.hyracks.dataflow.std.group.hash.HashGroupOperatorDescriptor;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;

public class LocalityAwareConnectorTest extends AbstractMultiNCIntegrationTest {

    final IFileSplitProvider splitProvider = new ConstantFileSplitProvider(new FileSplit[] {
            new FileSplit("asterix-001", new FileReference(new File("data/tpch0.001/lineitem.tbl"))),
            new FileSplit("asterix-002", new FileReference(new File("data/tpch0.001/lineitem.tbl"))),
            new FileSplit("asterix-003", new FileReference(new File("data/tpch0.001/lineitem.tbl"))),
            new FileSplit("asterix-004", new FileReference(new File("data/tpch0.001/lineitem.tbl"))) });

    final RecordDescriptor desc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            FloatSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    final ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
            UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
            IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
            FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, }, '|');

    /**
     * Test of aggregations using locality aware connector. The output two files should be the
     * same, each of which is the aggregation of two copies of the lineitem.tbl.
     * Note that if the hashing connector is not working correctly, the two files may be different. This
     * also means that even the output files are the same, the hashing may have other problems.
     * 
     * @throws Exception
     */
    @Test
    public void localityAwareAggregationTest() throws Exception {

        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, "asterix-001", "asterix-002",
                "asterix-003", "asterix-004");

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] { 0 };
        int tableSize = 8;

        HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(spec, keyFields,
                new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }),
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(1, true), new IntSumFieldAggregatorFactory(3, true),
                        new FloatSumFieldAggregatorFactory(5, true) }), outputRec, tableSize);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, "asterix-005", "asterix-006");

        BitSet nodemap = new BitSet(8);

        nodemap.set(0);
        nodemap.set(2);
        nodemap.set(5);
        nodemap.set(7);

        IConnectorDescriptor conn1 = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }), new HashtableLocalityMap(nodemap));

        spec.connect(conn1, csvScanner, 0, grouper, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "localityAwareSumInmemGroupTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, "asterix-005", "asterix-006");

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    /**
     * Test for locality aware connector, using the global hashing node mapper. This should have
     * the exactly the same result as using {@link MToNPartitioningConnectorDescriptor}.
     * 
     * @throws Exception
     */
    @Test
    public void globalPartitionAggregationTest() throws Exception {

        JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(spec, splitProvider, tupleParserFactory,
                desc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner, "asterix-001", "asterix-002",
                "asterix-003", "asterix-004");

        RecordDescriptor outputRec = new RecordDescriptor(new ISerializerDeserializer[] {
                new UTF8StringSerializerDeserializer(), IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE });

        int[] keyFields = new int[] { 0 };
        int tableSize = 8;

        HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(spec, keyFields,
                new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }),
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY) },
                new MultiFieldsAggregatorFactory(new IFieldAggregateDescriptorFactory[] {
                        new IntSumFieldAggregatorFactory(1, true), new IntSumFieldAggregatorFactory(3, true),
                        new FloatSumFieldAggregatorFactory(5, true) }), outputRec, tableSize);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, "asterix-005", "asterix-006");

        IConnectorDescriptor conn1 = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                new FieldHashPartitionComputerFactory(keyFields,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                .of(UTF8StringPointable.FACTORY) }), new GlobalHashingLocalityMap());

        spec.connect(conn1, csvScanner, 0, grouper, 0);

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec, "localityAwareSumInmemGroupTest");

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, "asterix-005", "asterix-006");

        IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn2, grouper, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
    }

    private AbstractSingleActivityOperatorDescriptor getPrinter(JobSpecification spec, String prefix)
            throws IOException {

        ResultSetId rsId = new ResultSetId(1);
        AbstractSingleActivityOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, true,false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());
        spec.addResultSetId(rsId);

        return printer;
    }

}
