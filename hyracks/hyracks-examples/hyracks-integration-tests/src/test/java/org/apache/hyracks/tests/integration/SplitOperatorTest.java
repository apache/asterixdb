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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import org.apache.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import org.apache.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.misc.SplitOperatorDescriptor;
import org.apache.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import org.apache.hyracks.tests.util.ResultSerializerFactoryProvider;

public class SplitOperatorTest extends AbstractIntegrationTest {

    public void compareFiles(String fileNameA, String fileNameB) throws IOException {
        BufferedReader fileA = new BufferedReader(new FileReader(fileNameA));
        BufferedReader fileB = new BufferedReader(new FileReader(fileNameB));

        String lineA, lineB;
        while ((lineA = fileA.readLine()) != null) {
            lineB = fileB.readLine();
            Assert.assertEquals(lineA, lineB);
        }
        Assert.assertNull(fileB.readLine());
        fileA.close();
        fileB.close();
    }

    @Test
    public void test() throws Exception {
        final int outputArity = 2;

        JobSpecification spec = new JobSpecification();

        String inputFileName = "data/words.txt";
        File[] outputFile = new File[outputArity];
        for (int i = 0; i < outputArity; i++) {
            outputFile[i] = File.createTempFile("splitop", null);
            outputFile[i].deleteOnExit();
        }

        FileSplit[] inputSplits = new FileSplit[] { new FileSplit(NC1_ID, inputFileName) };

        String[] locations = new String[] { NC1_ID };

        DelimitedDataTupleParserFactory stringParser = new DelimitedDataTupleParserFactory(
                new IValueParserFactory[] { UTF8StringParserFactory.INSTANCE }, '\u0000');
        RecordDescriptor stringRec = new RecordDescriptor(
                new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(), });

        FileScanOperatorDescriptor scanOp = new FileScanOperatorDescriptor(spec, new ConstantFileSplitProvider(
                inputSplits), stringParser, stringRec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, scanOp, locations);

        SplitOperatorDescriptor splitOp = new SplitOperatorDescriptor(spec, stringRec, outputArity);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, splitOp, locations);

        IOperatorDescriptor outputOp[] = new IOperatorDescriptor[outputFile.length];
        for (int i = 0; i < outputArity; i++) {
            ResultSetId rsId = new ResultSetId(i);
            spec.addResultSetId(rsId);

            outputOp[i] = new ResultWriterOperatorDescriptor(spec, rsId, true, false,
                    ResultSerializerFactoryProvider.INSTANCE.getResultSerializerFactoryProvider());
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, outputOp[i], locations);
        }

        spec.connect(new OneToOneConnectorDescriptor(spec), scanOp, 0, splitOp, 0);
        for (int i = 0; i < outputArity; i++) {
            spec.connect(new OneToOneConnectorDescriptor(spec), splitOp, i, outputOp[i], 0);
        }

        for (int i = 0; i < outputArity; i++) {
            spec.addRoot(outputOp[i]);
        }
        String[] expectedResultsFileNames = new String[outputArity];
        for (int i = 0; i < outputArity; i++) {
            expectedResultsFileNames[i] = inputFileName;
        }
        runTestAndCompareResults(spec, expectedResultsFileNames);
    }
}