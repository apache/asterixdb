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
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.VertexCombiner;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.runtime.base.IAggregateFunction;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class AggregationFunction implements IAggregateFunction {
    private final Configuration conf;
    private final boolean isFinalStage;
    private final DataOutput output;
    private VertexCombiner combiner;
    private ByteBufferInputStream keyInputStream = new ByteBufferInputStream();
    private ByteBufferInputStream valueInputStream = new ByteBufferInputStream();
    private DataInput keyInput = new DataInputStream(keyInputStream);
    private DataInput valueInput = new DataInputStream(valueInputStream);
    private WritableComparable key;
    private Writable value;
    private Writable combinedResult;
    private MsgList msgList = new MsgList();
    private boolean keyRead = false;

    public AggregationFunction(IConfigurationFactory confFactory, DataOutput output, boolean isFinalStage)
            throws HyracksDataException {
        this.conf = confFactory.createConfiguration();
        this.output = output;
        this.isFinalStage = isFinalStage;
        msgList.setConf(this.conf);

        combiner = BspUtils.createVertexCombiner(conf);
        key = BspUtils.createVertexIndex(conf);
        value = BspUtils.createMessageValue(conf);
        combinedResult = BspUtils.createMessageValue(conf);
    }

    @Override
    public void init() throws HyracksDataException {
        msgList.clear();
        keyRead = false;
        combiner.init();
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
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
                keyRead = false;
            }
            value.readFields(valueInput);
            combiner.step(key, value);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }

    }

    @Override
    public void finish() throws HyracksDataException {
        try {
            combinedResult = combiner.finish();
            if (!isFinalStage) {
                combinedResult.write(output);
            } else {
                msgList.add(combinedResult);
                msgList.write(output);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
