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
package edu.uci.ics.pregelix.runtime.agg;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.ISerializableAggregateFunction;
import edu.uci.ics.pregelix.dataflow.std.util.ResetableByteArrayOutputStream;

@SuppressWarnings("rawtypes")
public class SerializableAggregateFunction implements ISerializableAggregateFunction {
    private final Configuration conf;
    private final boolean partialAggAsInput;
    private MessageCombiner combiner;
    private ByteBufferInputStream keyInputStream = new ByteBufferInputStream();
    private ByteBufferInputStream valueInputStream = new ByteBufferInputStream();
    private ByteBufferInputStream stateInputStream = new ByteBufferInputStream();
    private DataInput keyInput = new DataInputStream(keyInputStream);
    private DataInput valueInput = new DataInputStream(valueInputStream);
    private DataInput stateInput = new DataInputStream(stateInputStream);
    private ResetableByteArrayOutputStream stateBos = new ResetableByteArrayOutputStream();
    private DataOutput stateOutput = new DataOutputStream(stateBos);
    private WritableComparable key;
    private Writable value;
    private Writable combinedResult;
    private Writable finalResult;
    private MsgList msgList = new MsgList();

    public SerializableAggregateFunction(IHyracksTaskContext ctx, IConfigurationFactory confFactory,
            boolean partialAggAsInput) throws HyracksDataException {
        this.conf = confFactory.createConfiguration(ctx);
        this.partialAggAsInput = partialAggAsInput;
        msgList.setConf(this.conf);

        combiner = BspUtils.createMessageCombiner(conf);
        key = BspUtils.createVertexIndex(conf);
        value = !partialAggAsInput ? BspUtils.createMessageValue(conf) : BspUtils.createPartialCombineValue(conf);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(IFrameTupleReference tuple, ArrayTupleBuilder state) throws HyracksDataException {
        try {
            /**
             * bind key and value
             */
            bindKeyValue(tuple);
            key.readFields(keyInput);
            value.readFields(valueInput);

            combiner.init(msgList);

            /**
             * call the step function of the aggregator
             */
            if (!partialAggAsInput) {
                combiner.stepPartial(key, (WritableSizable) value);
            } else {
                combiner.stepFinal(key, (WritableSizable) value);
            }

            /**
             * output state to the array tuple builder
             */
            combinedResult = combiner.finishPartial();
            combinedResult.write(state.getDataOutput());
            state.addFieldEndOffset();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void step(IFrameTupleReference tuple, IFrameTupleReference state) throws HyracksDataException {
        try {
            /**
             * bind key and value
             */
            bindKeyValue(tuple);
            key.readFields(keyInput);
            value.readFields(valueInput);

            /**
             * bind state
             */
            bindState(state);
            combinedResult.readFields(stateInput);

            /**
             * set the partial state
             */
            combiner.setPartialCombineState(combinedResult);

            /**
             * call the step function of the aggregator
             */
            if (!partialAggAsInput) {
                combiner.stepPartial(key, (WritableSizable) value);
            } else {
                combiner.stepFinal(key, (WritableSizable) value);
            }

            /**
             * write out partial state
             */
            combinedResult = combiner.finishPartial();
            combinedResult.write(stateOutput);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public void finishPartial(IFrameTupleReference state, ArrayTupleBuilder output) throws HyracksDataException {
        try {
            /**
             * bind state
             */
            bindState(state);
            combinedResult.readFields(stateInput);

            /**
             * set the partial state
             */
            combiner.setPartialCombineState(combinedResult);
            combinedResult = combiner.finishPartial();
            combinedResult.write(output.getDataOutput());
            output.addFieldEndOffset();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public void finishFinal(IFrameTupleReference state, ArrayTupleBuilder output) throws HyracksDataException {
        try {
            /**
             * bind key and value
             */
            bindKeyValue(state);
            key.readFields(keyInput);

            /**
             * bind state
             */
            bindState(state);
            combinedResult.readFields(stateInput);

            /**
             * set the partial state
             */
            if (!partialAggAsInput) {
                combiner.setPartialCombineState(combinedResult);
                combinedResult = combiner.finishPartial();
                combinedResult.write(output.getDataOutput());
            } else {
                combiner.setPartialCombineState(combinedResult);
                finalResult = combiner.finishFinal();
                finalResult.write(output.getDataOutput());
            }
            output.addFieldEndOffset();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * @param state
     */
    private void bindState(IFrameTupleReference state) {
        FrameTupleReference ftr = (FrameTupleReference) state;
        IFrameTupleAccessor fta = ftr.getFrameTupleAccessor();
        ByteBuffer buffer = fta.getBuffer();
        int tIndex = ftr.getTupleIndex();
        int combinedStateStart = fta.getFieldSlotsLength() + fta.getTupleStartOffset(tIndex)
                + fta.getFieldStartOffset(tIndex, 1);
        stateInputStream.setByteBuffer(buffer, combinedStateStart);
        stateBos.setByteArray(buffer.array(), combinedStateStart);
    }

    /**
     * @param tuple
     */
    private void bindKeyValue(IFrameTupleReference tuple) {
        FrameTupleReference ftr = (FrameTupleReference) tuple;
        IFrameTupleAccessor fta = ftr.getFrameTupleAccessor();
        ByteBuffer buffer = fta.getBuffer();
        int tIndex = ftr.getTupleIndex();
        int keyStart = fta.getFieldSlotsLength() + fta.getTupleStartOffset(tIndex) + fta.getFieldStartOffset(tIndex, 0);
        int valueStart = fta.getFieldSlotsLength() + fta.getTupleStartOffset(tIndex)
                + fta.getFieldStartOffset(tIndex, 1);
        keyInputStream.setByteBuffer(buffer, keyStart);
        valueInputStream.setByteBuffer(buffer, valueStart);
    }

}
