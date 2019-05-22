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
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.ManagedFileSplit;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.dataflow.std.union.UnionAllOperatorDescriptor;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;
import org.junit.Test;

public class UnionTest extends AbstractIntegrationTest {
    @Test
    public void union01() throws Exception {
        JobSpecification spec = createUnionJobSpec();
        runTest(spec);
    }

    public static JobSpecification createUnionJobSpec() throws Exception {
        JobSpecification spec = new JobSpecification();

        IFileSplitProvider splitProvider = new ConstantFileSplitProvider(
                new FileSplit[] { new ManagedFileSplit(NC2_ID, "data" + File.separator + "words.txt"),
                        new ManagedFileSplit(NC1_ID, "data" + File.separator + "nc1" + File.separator + "words.txt") });

        RecordDescriptor desc =
                new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer() });

        FileScanOperatorDescriptor csvScanner01 =
                new FileScanOperatorDescriptor(spec, splitProvider, new DelimitedDataTupleParserFactory(
                        new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','), desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner01, NC2_ID, NC1_ID);

        FileScanOperatorDescriptor csvScanner02 =
                new FileScanOperatorDescriptor(spec, splitProvider, new DelimitedDataTupleParserFactory(
                        new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, ','), desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, csvScanner02, NC2_ID, NC1_ID);

        UnionAllOperatorDescriptor unionAll = new UnionAllOperatorDescriptor(spec, 2, desc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, unionAll, NC2_ID, NC1_ID);

        ResultSetId rsId = new ResultSetId(1);
        spec.addResultSetId(rsId);

        IOperatorDescriptor printer = new ResultWriterOperatorDescriptor(spec, rsId, null, false,
                ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider(), 1);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC2_ID, NC1_ID);

        spec.connect(new OneToOneConnectorDescriptor(spec), csvScanner01, 0, unionAll, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), csvScanner02, 0, unionAll, 1);
        spec.connect(new OneToOneConnectorDescriptor(spec), unionAll, 0, printer, 0);

        spec.addRoot(printer);
        return spec;
    }
}
