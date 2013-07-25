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

package edu.uci.ics.pregelix.runtime.simpleagg;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunction;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class AggregationFunction implements IAggregateFunction {
    private final Configuration conf;
    private final boolean isFinalStage;
    private final boolean partialAggAsInput;
    private final DataOutput output;
    private MessageCombiner combiner;
    private ByteBufferInputStream keyInputStream = new ByteBufferInputStream();
    private ByteBufferInputStream valueInputStream = new ByteBufferInputStream();
    private DataInput keyInput = new DataInputStream(keyInputStream);
    private DataInput valueInput = new DataInputStream(valueInputStream);
    private WritableComparable key;
    private Writable value;
    private Writable combinedResult;
    private MsgList msgList = new MsgList();
    private boolean keyRead = false;

    public AggregationFunction(IHyracksTaskContext ctx, IConfigurationFactory confFactory, DataOutput tmpOutput,
            IFrameWriter groupByOutputWriter, boolean isFinalStage, boolean partialAggAsInput)
            throws HyracksDataException {
        this.conf = confFactory.createConfiguration(ctx);
        this.output = tmpOutput;
        this.isFinalStage = isFinalStage;
        this.partialAggAsInput = partialAggAsInput;
        msgList.setConf(this.conf);

        combiner = BspUtils.createMessageCombiner(conf);
        key = BspUtils.createVertexIndex(conf);
        value = !partialAggAsInput ? BspUtils.createMessageValue(conf) : BspUtils.createPartialCombineValue(conf);
    }

    @Override
    public void initAll() throws HyracksDataException {
        keyRead = false;
        combiner.initAll(msgList);
    }

    @Override
    public void init() throws HyracksDataException {
        keyRead = false;
        combiner.init(msgList);
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        if (!partialAggAsInput) {
            combiner.stepPartial(key, (WritableSizable) value);
        } else {
            combiner.stepFinal(key, value);
        }
    }

    @Override
    public void finish() throws HyracksDataException {
        try {
            if (!isFinalStage) {
                combinedResult = combiner.finishPartial();
            } else {
                combinedResult = combiner.finishFinal();
            }
            combinedResult.write(output);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void finishAll() throws HyracksDataException {
        try {
            if (!isFinalStage) {
                combinedResult = combiner.finishPartial();
            } else {
                combinedResult = combiner.finishFinalAll();
            }
            combinedResult.write(output);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public int estimateStep(IFrameTupleReference tuple) throws HyracksDataException {
        FrameTupleReference ftr = (FrameTupleReference) tuple;
        IFrameTupleAccessor fta = ftr.getFrameTupleAccessor();
        ByteBuffer buffer = fta.getBuffer();
        int tIndex = ftr.getTupleIndex();

        int keyStart = fta.getFieldSlotsLength() + fta.getTupleStartOffset(tIndex) + fta.getFieldStartOffset(tIndex, 0);
        int valueStart = fta.getFieldSlotsLength() + fta.getTupleStartOffset(tIndex)
                + fta.getFieldStartOffset(tIndex, 1);

        keyInputStream.setByteBuffer(buffer, keyStart);
        valueInputStream.setByteBuffer(buffer, valueStart);

        try {
            if (!keyRead) {
                key.readFields(keyInput);
                keyRead = true;
            }
            value.readFields(valueInput);
            if (!partialAggAsInput) {
                return combiner.estimateAccumulatedStateByteSizePartial(key, (WritableSizable) value);
            } else {
                return combiner.estimateAccumulatedStateByteSizeFinal(key, value);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
