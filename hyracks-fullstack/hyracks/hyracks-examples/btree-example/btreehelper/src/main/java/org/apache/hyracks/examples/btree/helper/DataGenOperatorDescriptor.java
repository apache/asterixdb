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

package org.apache.hyracks.examples.btree.helper;

import java.io.DataOutput;
import java.util.HashSet;
import java.util.Random;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class DataGenOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int numRecords;

    private final int intMinVal;
    private final int intMaxVal;
    private final int maxStrLen;
    private final int uniqueField;
    private final long randomSeed;

    public DataGenOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor outputRecord, int numRecords,
            int uniqueField, int intMinVal, int intMaxVal, int maxStrLen, long randomSeed) {
        super(spec, 0, 1);
        this.numRecords = numRecords;
        this.uniqueField = uniqueField;
        this.intMinVal = intMinVal;
        this.intMaxVal = intMaxVal;
        this.maxStrLen = maxStrLen;
        this.randomSeed = randomSeed;
        outRecDescs[0] = outputRecord;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        final FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
        final RecordDescriptor recDesc = outRecDescs[0];
        final ArrayTupleBuilder tb = new ArrayTupleBuilder(recDesc.getFields().length);
        final Random rnd = new Random(randomSeed);
        final int maxUniqueAttempts = 20;

        return new AbstractUnaryOutputSourceOperatorNodePushable() {

            // for quick & dirty exclusion of duplicates
            // WARNING: could contain numRecord entries and use a lot of memory
            HashSet<String> stringHs = new HashSet<String>();
            HashSet<Integer> intHs = new HashSet<Integer>();

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    writer.open();
                    for (int i = 0; i < numRecords; i++) {
                        tb.reset();
                        for (int j = 0; j < recDesc.getFieldCount(); j++) {
                            genField(tb, j);
                        }

                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            appender.write(writer, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new HyracksDataException("Record size (" + tb.getSize()
                                        + ") larger than frame size (" + appender.getBuffer().capacity() + ")");
                            }
                        }
                    }
                    appender.write(writer, true);
                } catch (Throwable th) {
                    writer.fail();
                    throw HyracksDataException.create(th);
                } finally {
                    writer.close();
                }
            }

            private void genField(ArrayTupleBuilder tb, int fieldIndex) throws HyracksDataException {
                DataOutput dos = tb.getDataOutput();
                if (recDesc.getFields()[fieldIndex] instanceof IntegerSerializerDeserializer) {
                    int val = -1;
                    if (fieldIndex == uniqueField) {
                        int attempt = 0;
                        while (attempt < maxUniqueAttempts) {
                            int tmp = Math.abs(rnd.nextInt()) % (intMaxVal - intMinVal) + intMinVal;
                            if (intHs.contains(tmp))
                                attempt++;
                            else {
                                val = tmp;
                                intHs.add(val);
                                break;
                            }
                        }
                        if (attempt == maxUniqueAttempts)
                            throw new HyracksDataException("MaxUnique attempts reached in datagen");
                    } else {
                        val = Math.abs(rnd.nextInt()) % (intMaxVal - intMinVal) + intMinVal;
                    }
                    recDesc.getFields()[fieldIndex].serialize(val, dos);
                    tb.addFieldEndOffset();
                } else if (recDesc.getFields()[fieldIndex] instanceof UTF8StringSerializerDeserializer) {
                    String val = null;
                    if (fieldIndex == uniqueField) {
                        int attempt = 0;
                        while (attempt < maxUniqueAttempts) {
                            String tmp = randomString(maxStrLen, rnd);
                            if (stringHs.contains(tmp))
                                attempt++;
                            else {
                                val = tmp;
                                stringHs.add(val);
                                break;
                            }
                        }
                        if (attempt == maxUniqueAttempts)
                            throw new HyracksDataException("MaxUnique attempts reached in datagen");
                    } else {
                        val = randomString(maxStrLen, rnd);
                    }
                    recDesc.getFields()[fieldIndex].serialize(val, dos);
                    tb.addFieldEndOffset();
                } else {
                    throw new HyracksDataException(
                            "Type unsupported in data generator. Only integers and strings allowed");
                }
            }

            private String randomString(int length, Random random) {
                String s = Long.toHexString(Double.doubleToLongBits(random.nextDouble()));
                StringBuilder strBuilder = new StringBuilder();
                for (int i = 0; i < s.length() && i < length; i++) {
                    strBuilder.append(s.charAt(Math.abs(random.nextInt()) % s.length()));
                }
                return strBuilder.toString();
            }
        };
    }
}
