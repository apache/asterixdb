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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.misc.SplitOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.result.ResultWriterOperatorDescriptor;
import edu.uci.ics.hyracks.tests.util.ResultSerializerFactoryProvider;

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
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE, });

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