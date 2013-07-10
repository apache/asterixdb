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

package edu.uci.ics.hyracks.examples.btree.helper;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Random;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

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
        recordDescriptors[0] = outputRecord;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        final ByteBuffer outputFrame = ctx.allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        final RecordDescriptor recDesc = recordDescriptors[0];
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
                writer.open();
                try {
                    appender.reset(outputFrame, true);
                    for (int i = 0; i < numRecords; i++) {
                        tb.reset();
                        for (int j = 0; j < recDesc.getFieldCount(); j++) {
                            genField(tb, j);
                        }

                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            FrameUtils.flushFrame(outputFrame, writer);
                            appender.reset(outputFrame, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size (" + appender.getBuffer().capacity() + ")");
                            }
                        }
                    }
                    FrameUtils.flushFrame(outputFrame, writer);
                } catch (Exception e) {
                    writer.fail();
                    throw new HyracksDataException(e);
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
