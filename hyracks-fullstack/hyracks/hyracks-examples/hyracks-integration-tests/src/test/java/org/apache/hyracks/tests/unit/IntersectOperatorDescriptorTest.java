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

package org.apache.hyracks.tests.unit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.intersect.IntersectOperatorDescriptor;
import org.apache.hyracks.test.support.TestUtils;
import org.apache.hyracks.tests.util.InputFrameGenerator;
import org.apache.hyracks.tests.util.MultiThreadTaskEmulator;
import org.apache.hyracks.tests.util.OutputFrameVerifier;
import org.junit.Before;
import org.junit.Test;

public class IntersectOperatorDescriptorTest {

    IOperatorDescriptorRegistry mockRegistry =
            when(mock(IOperatorDescriptorRegistry.class).createOperatorDescriptorId(any()))
                    .thenReturn(new OperatorDescriptorId(1)).getMock();
    MultiThreadTaskEmulator multiThreadTaskEmulator = new MultiThreadTaskEmulator();
    InputFrameGenerator frameGenerator = new InputFrameGenerator(256);
    IHyracksTaskContext ctx = TestUtils.create(256);

    int nInputs;
    int nProjectFields;
    int[][] compareFields;
    RecordDescriptor[] inputRecordDescriptor;
    INormalizedKeyComputerFactory normalizedKeyFactory;
    IBinaryComparatorFactory[] comparatorFactory;
    RecordDescriptor outRecordDescriptor;

    protected void initializeParameters() {
        compareFields = new int[nInputs][];

        inputRecordDescriptor = new RecordDescriptor[nInputs];

        normalizedKeyFactory = null;
        comparatorFactory = new IBinaryComparatorFactory[] { IntegerBinaryComparatorFactory.INSTANCE,
                IntegerBinaryComparatorFactory.INSTANCE };

        for (int i = 0; i < nInputs; i++) {
            compareFields[i] = new int[nProjectFields];
            for (int f = 0; f < nProjectFields; f++) {
                compareFields[i][f] = f;
            }
        }
        for (int i = 0; i < nInputs; i++) {
            inputRecordDescriptor[i] =
                    new RecordDescriptor(new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE,
                            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        }

        outRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
    }

    @Before
    public void setUpInput() {
        nInputs = 3;
        nProjectFields = 2;
        initializeParameters();
    }

    @Test
    public void testNormalOperatorInitialization() throws HyracksException {

        IntersectOperatorDescriptor operatorDescriptor = new IntersectOperatorDescriptor(mockRegistry, nInputs,
                compareFields, null, normalizedKeyFactory, comparatorFactory, outRecordDescriptor);

        assertEquals(nInputs, operatorDescriptor.getInputArity());
    }

    @Test
    public void testCommonIntersect() throws Exception {
        List<Object[]> answer = new ArrayList<>();
        List<IFrame>[] inputFrames = new ArrayList[nInputs];
        prepareCommonDataFrame(inputFrames, answer);
        executeAndVerifyResult(inputFrames, answer);
    }

    @Test
    public void testNullOutputIntersect() throws Exception {
        List<Object[]> answer = new ArrayList<>();
        List<IFrame>[] inputFrames = new ArrayList[nInputs];
        prepareNullResultDataFrame(inputFrames, answer);
        executeAndVerifyResult(inputFrames, answer);
    }

    @Test
    public void testOneInputIsVeryShortIntersect() throws Exception {
        List<Object[]> answer = new ArrayList<>();
        List<IFrame>[] inputFrames = new ArrayList[nInputs];
        prepareOneInputIsVeryShortDataFrame(inputFrames, answer);
        executeAndVerifyResult(inputFrames, answer);
    }

    @Test
    public void testAllSameInputIntersect() throws Exception {
        List<Object[]> answer = new ArrayList<>();
        List<IFrame>[] inputFrames = new ArrayList[nInputs];
        prepareAllSameInputDataFrame(inputFrames, answer);
        executeAndVerifyResult(inputFrames, answer);
    }

    @Test
    public void testOnlyOneInputIntersect() throws Exception {
        nInputs = 1;
        initializeParameters();
        List<Object[]> answer = new ArrayList<>();
        List<IFrame>[] inputFrames = new ArrayList[nInputs];
        prepareAllSameInputDataFrame(inputFrames, answer);
        executeAndVerifyResult(inputFrames, answer);
    }

    private void executeAndVerifyResult(List<IFrame>[] inputFrames, List<Object[]> answer) throws Exception {
        IntersectOperatorDescriptor.IntersectOperatorNodePushable pushable =
                new IntersectOperatorDescriptor.IntersectOperatorNodePushable(ctx, nInputs, inputRecordDescriptor,
                        compareFields, null, null, comparatorFactory);
        assertEquals(nInputs, pushable.getInputArity());

        IFrameWriter[] writers = new IFrameWriter[nInputs];
        for (int i = 0; i < nInputs; i++) {
            writers[i] = pushable.getInputFrameWriter(i);
        }
        IFrameWriter resultVerifier = new OutputFrameVerifier(outRecordDescriptor, answer);
        pushable.setOutputFrameWriter(0, resultVerifier, outRecordDescriptor);
        multiThreadTaskEmulator.runInParallel(writers, inputFrames);
    }

    protected void prepareCommonDataFrame(List<IFrame>[] inputFrames, List<Object[]> answer)
            throws HyracksDataException {
        for (int i = 0; i < nInputs; i++) {
            List<Object[]> inputObjects = new ArrayList<>();
            generateRecordStream(inputObjects, inputRecordDescriptor[i], i + 1, (i + 1) * 100, 1);
            inputFrames[i] = frameGenerator.generateDataFrame(inputRecordDescriptor[i], inputObjects);
        }
        generateRecordStream(answer, outRecordDescriptor, nInputs, 100, 1);
    }

    protected void prepareNullResultDataFrame(List<IFrame>[] inputFrames, List<Object[]> answer)
            throws HyracksDataException {
        for (int i = 0; i < nInputs; i++) {
            List<Object[]> inputObjects = new ArrayList<>();
            generateRecordStream(inputObjects, inputRecordDescriptor[i], (i + 1) * 100, (i + 2) * 100, 1);
            inputFrames[i] = frameGenerator.generateDataFrame(inputRecordDescriptor[i], inputObjects);
        }
    }

    protected void prepareOneInputIsVeryShortDataFrame(List<IFrame>[] inputFrames, List<Object[]> answer)
            throws HyracksDataException {
        for (int i = 0; i < nInputs; i++) {
            List<Object[]> inputObjects = new ArrayList<>();
            generateRecordStream(inputObjects, inputRecordDescriptor[i], i, i * 100 + 1, 1);
            inputFrames[i] = frameGenerator.generateDataFrame(inputRecordDescriptor[i], inputObjects);
        }
    }

    protected void prepareAllSameInputDataFrame(List<IFrame>[] inputFrames, List<Object[]> answer)
            throws HyracksDataException {
        for (int i = 0; i < nInputs; i++) {
            List<Object[]> inputObjects = new ArrayList<>();
            generateRecordStream(inputObjects, inputRecordDescriptor[i], 0, 100, 1);
            inputFrames[i] = frameGenerator.generateDataFrame(inputRecordDescriptor[i], inputObjects);
        }
        generateRecordStream(answer, outRecordDescriptor, 0, 100, 1);
    }

    private void generateRecordStream(List<Object[]> inputs, RecordDescriptor recordDesc, int start, int end,
            int step) {
        for (int i = start; i < end; i += step) {
            Object[] obj = new Object[recordDesc.getFieldCount()];
            for (int f = 0; f < recordDesc.getFieldCount(); f++) {
                obj[f] = i;
            }
            inputs.add(obj);
        }
    }

}
