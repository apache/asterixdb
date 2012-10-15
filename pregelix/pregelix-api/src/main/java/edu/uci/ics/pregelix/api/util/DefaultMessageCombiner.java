package edu.uci.ics.pregelix.api.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class DefaultMessageCombiner<I extends WritableComparable, M extends Writable> extends
        MessageCombiner<I, M, MsgList> {
    private MsgList<M> msgList;

    @Override
    public void init(MsgList providedMsgList) {
        this.msgList = providedMsgList;
        this.msgList.clearElements();
    }

    @Override
    public void step(I vertexIndex, M msg) throws HyracksDataException {
        msgList.addElement(msg);
    }

    @Override
    public void step(MsgList partialAggregate) throws HyracksDataException {
        msgList.addAllElements(partialAggregate);
    }

    @Override
    public MsgList finishPartial() {
        return msgList;
    }

    @Override
    public MsgList<M> finishFinal() {
        return msgList;
    }

}
