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
import java.io.IOException;

import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.comparators.UTF8StringBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.PreclusteredGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.AvgFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MinMaxStringFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;

/**
 *
 */
public class AggregationTests extends AbstractIntegrationTest {

	final IFileSplitProvider splitProvider = new ConstantFileSplitProvider(
			new FileSplit[] { new FileSplit(NC2_ID, new FileReference(new File(
					"data/tpch0.001/lineitem.tbl"))) });

	final RecordDescriptor desc = new RecordDescriptor(
			new ISerializerDeserializer[] {
					UTF8StringSerializerDeserializer.INSTANCE,
					IntegerSerializerDeserializer.INSTANCE,
					IntegerSerializerDeserializer.INSTANCE,
					IntegerSerializerDeserializer.INSTANCE,
					IntegerSerializerDeserializer.INSTANCE,
					FloatSerializerDeserializer.INSTANCE,
					FloatSerializerDeserializer.INSTANCE,
					FloatSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE,
					UTF8StringSerializerDeserializer.INSTANCE });

	final ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(
			new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE,
					IntegerParserFactory.INSTANCE,
					IntegerParserFactory.INSTANCE,
					IntegerParserFactory.INSTANCE,
					IntegerParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
					FloatParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
					UTF8StringParserFactory.INSTANCE,
					UTF8StringParserFactory.INSTANCE,
					UTF8StringParserFactory.INSTANCE,
					UTF8StringParserFactory.INSTANCE,
					UTF8StringParserFactory.INSTANCE,
					UTF8StringParserFactory.INSTANCE,
					UTF8StringParserFactory.INSTANCE,
					UTF8StringParserFactory.INSTANCE, }, '|');

	private AbstractSingleActivityOperatorDescriptor getPrinter(
			JobSpecification spec, String prefix) throws IOException {

		AbstractSingleActivityOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(
				spec, new ConstantFileSplitProvider(new FileSplit[] {
						new FileSplit(NC1_ID, createTempFile()
								.getAbsolutePath()),
						new FileSplit(NC2_ID, createTempFile()
								.getAbsolutePath()) }), "\t");

		return printer;
	}

	@Test
	public void singleKeySumInmemGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };
		int tableSize = 8;

		HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(
				spec,
				keyFields,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new IntSumFieldAggregatorFactory(3, true) }),
				outputRec, tableSize);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeySumInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void singleKeySumPreClusterGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };

		PreclusteredGroupOperatorDescriptor grouper = new PreclusteredGroupOperatorDescriptor(
				spec,
				keyFields,
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new IntSumFieldAggregatorFactory(3, true) }),
				outputRec);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeySumInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void singleKeySumExtGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };
		int frameLimits = 4;
		int tableSize = 8;

		ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(
				spec,
				keyFields,
				frameLimits,
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new UTF8StringNormalizedKeyComputerFactory(),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new IntSumFieldAggregatorFactory(3, false) }),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new IntSumFieldAggregatorFactory(2, false) }),
				outputRec,
				new HashSpillableTableFactory(
						new FieldHashPartitionComputerFactory(
								keyFields,
								new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
						tableSize), true);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeySumExtGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void singleKeyAvgInmemGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						FloatSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };
		int tableSize = 8;

		HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(
				spec,
				keyFields,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new CountFieldAggregatorFactory(true),
								new AvgFieldAggregatorFactory(1, true) }),
				outputRec, tableSize);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeyAvgInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void singleKeyAvgPreClusterGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						FloatSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };

		PreclusteredGroupOperatorDescriptor grouper = new PreclusteredGroupOperatorDescriptor(
				spec,
				keyFields,
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new CountFieldAggregatorFactory(true),
								new AvgFieldAggregatorFactory(1, true) }),
				outputRec);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeyAvgInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void singleKeyAvgExtGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						FloatSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };
		int frameLimits = 4;
		int tableSize = 8;

		ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(
				spec,
				keyFields,
				frameLimits,
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new UTF8StringNormalizedKeyComputerFactory(),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new CountFieldAggregatorFactory(false),
								new AvgFieldAggregatorFactory(1, false) }),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new IntSumFieldAggregatorFactory(2, false),
								new AvgFieldAggregatorFactory(3, false) }),
				outputRec,
				new HashSpillableTableFactory(
						new FieldHashPartitionComputerFactory(
								keyFields,
								new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
						tableSize), true);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeyAvgExtGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void singleKeyMinMaxStringInmemGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };
		int tableSize = 8;

		HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(
				spec,
				keyFields,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new MinMaxStringFieldAggregatorFactory(15,
										true, false) }), outputRec, tableSize);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeyAvgInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void singleKeyMinMaxStringPreClusterGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };

		PreclusteredGroupOperatorDescriptor grouper = new PreclusteredGroupOperatorDescriptor(
				spec,
				keyFields,
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new MinMaxStringFieldAggregatorFactory(15,
										true, false) }), outputRec);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeyAvgInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void singleKeyMinMaxStringExtGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 0 };
		int frameLimits = 4;
		int tableSize = 8;

		ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(
				spec,
				keyFields,
				frameLimits,
				new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE },
				new UTF8StringNormalizedKeyComputerFactory(),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new MinMaxStringFieldAggregatorFactory(15,
										true, true) }),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new MinMaxStringFieldAggregatorFactory(2, true,
										true) }),
				outputRec,
				new HashSpillableTableFactory(
						new FieldHashPartitionComputerFactory(
								keyFields,
								new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }),
						tableSize), true);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec,
				new FieldHashPartitionComputerFactory(
						keyFields,
						new IBinaryHashFunctionFactory[] { UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"singleKeyAvgExtGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeySumInmemGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };
		int tableSize = 8;

		HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(
				spec,
				keyFields,
				new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }),
				new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new IntSumFieldAggregatorFactory(3, true) }),
				outputRec, tableSize);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeySumInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeySumPreClusterGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };

		PreclusteredGroupOperatorDescriptor grouper = new PreclusteredGroupOperatorDescriptor(
				spec, keyFields, new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new IntSumFieldAggregatorFactory(3, true) }),
				outputRec);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeySumInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeySumExtGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };
		int frameLimits = 4;
		int tableSize = 8;

		ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(
				spec,
				keyFields,
				frameLimits,
				new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new UTF8StringNormalizedKeyComputerFactory(),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new IntSumFieldAggregatorFactory(3, false) }),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(2, false),
								new IntSumFieldAggregatorFactory(3, false) }),
				outputRec,
				new HashSpillableTableFactory(
						new FieldHashPartitionComputerFactory(
								keyFields,
								new IBinaryHashFunctionFactory[] {
										UTF8StringBinaryHashFunctionFactory.INSTANCE,
										UTF8StringBinaryHashFunctionFactory.INSTANCE }),
						tableSize), true);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeySumExtGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeyAvgInmemGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						FloatSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };
		int tableSize = 8;

		HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(
				spec,
				keyFields,
				new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }),
				new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new CountFieldAggregatorFactory(true),
								new AvgFieldAggregatorFactory(1, true) }),
				outputRec, tableSize);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeyAvgInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeyAvgPreClusterGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						FloatSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };

		PreclusteredGroupOperatorDescriptor grouper = new PreclusteredGroupOperatorDescriptor(
				spec, keyFields, new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new CountFieldAggregatorFactory(true),
								new AvgFieldAggregatorFactory(1, true) }),
				outputRec);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeyAvgInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeyAvgExtGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						FloatSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };
		int frameLimits = 4;
		int tableSize = 8;

		ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(
				spec,
				keyFields,
				frameLimits,
				new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new UTF8StringNormalizedKeyComputerFactory(),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new CountFieldAggregatorFactory(false),
								new AvgFieldAggregatorFactory(1, false) }),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(2, false),
								new IntSumFieldAggregatorFactory(3, false),
								new AvgFieldAggregatorFactory(4, false) }),
				outputRec,
				new HashSpillableTableFactory(
						new FieldHashPartitionComputerFactory(
								keyFields,
								new IBinaryHashFunctionFactory[] {
										UTF8StringBinaryHashFunctionFactory.INSTANCE,
										UTF8StringBinaryHashFunctionFactory.INSTANCE }),
						tableSize), true);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeyAvgExtGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeyMinMaxStringInmemGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };
		int tableSize = 8;

		HashGroupOperatorDescriptor grouper = new HashGroupOperatorDescriptor(
				spec,
				keyFields,
				new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }),
				new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new MinMaxStringFieldAggregatorFactory(15,
										true, false) }), outputRec, tableSize);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeyMinMaxStringInmemGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeyMinMaxStringPreClusterGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };

		PreclusteredGroupOperatorDescriptor grouper = new PreclusteredGroupOperatorDescriptor(
				spec, keyFields, new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, true),
								new MinMaxStringFieldAggregatorFactory(15,
										true, false) }), outputRec);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeyMinMaxStringPreClusterGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

	@Test
	public void multiKeyMinMaxStringExtGroupTest() throws Exception {
		JobSpecification spec = new JobSpecification();

		FileScanOperatorDescriptor csvScanner = new FileScanOperatorDescriptor(
				spec, splitProvider, tupleParserFactory, desc);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec,
				csvScanner, NC2_ID);

		RecordDescriptor outputRec = new RecordDescriptor(
				new ISerializerDeserializer[] {
						UTF8StringSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE,
						IntegerSerializerDeserializer.INSTANCE,
						UTF8StringSerializerDeserializer.INSTANCE });

		int[] keyFields = new int[] { 8, 0 };
		int frameLimits = 4;
		int tableSize = 8;

		ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(
				spec,
				keyFields,
				frameLimits,
				new IBinaryComparatorFactory[] {
						UTF8StringBinaryComparatorFactory.INSTANCE,
						UTF8StringBinaryComparatorFactory.INSTANCE },
				new UTF8StringNormalizedKeyComputerFactory(),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(1, false),
								new MinMaxStringFieldAggregatorFactory(15,
										true, true) }),
				new MultiFieldsAggregatorFactory(
						new IFieldAggregateDescriptorFactory[] {
								new IntSumFieldAggregatorFactory(2, false),
								new MinMaxStringFieldAggregatorFactory(3, true,
										true) }),
				outputRec,
				new HashSpillableTableFactory(
						new FieldHashPartitionComputerFactory(
								keyFields,
								new IBinaryHashFunctionFactory[] {
										UTF8StringBinaryHashFunctionFactory.INSTANCE,
										UTF8StringBinaryHashFunctionFactory.INSTANCE }),
						tableSize), true);

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn1 = new MToNPartitioningConnectorDescriptor(
				spec, new FieldHashPartitionComputerFactory(keyFields,
						new IBinaryHashFunctionFactory[] {
								UTF8StringBinaryHashFunctionFactory.INSTANCE,
								UTF8StringBinaryHashFunctionFactory.INSTANCE }));
		spec.connect(conn1, csvScanner, 0, grouper, 0);

		AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
				"multiKeyMinMaxStringExtGroupTest");

		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
				NC2_ID, NC1_ID);

		IConnectorDescriptor conn2 = new OneToOneConnectorDescriptor(spec);
		spec.connect(conn2, grouper, 0, printer, 0);

		spec.addRoot(printer);
		runTest(spec);
	}

}
