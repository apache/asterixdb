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
package edu.uci.ics.pregelix.api.util;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.io.WritableSizable;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class DefaultMessageCombiner<I extends WritableComparable, M extends WritableSizable> extends
        MessageCombiner<I, M, MsgList> {
    private MsgList<M> msgList;
    private int metaSlot = 8;
    private int accumulatedSize = metaSlot;

    @Override
    public void init(MsgList providedMsgList) {
        realInit(providedMsgList);
        this.msgList.setSegmentStart(false);
    }

    private void realInit(MsgList providedMsgList) {
        this.msgList = providedMsgList;
        this.msgList.clearElements();
        this.accumulatedSize = metaSlot;
    }

    @Override
    public void stepPartial(I vertexIndex, M msg) throws HyracksDataException {
        msgList.addElement(msg);
        accumulatedSize += msg.sizeInBytes();
    }

    @Override
    public void stepFinal(I vertexIndex, MsgList partialAggregate) throws HyracksDataException {
        msgList.addAllElements(partialAggregate);
        for (int i = 0; i < partialAggregate.size(); i++) {
            accumulatedSize += ((M) partialAggregate.get(i)).sizeInBytes();
        }
    }

    @Override
    public MsgList finishPartial() {
        msgList.setSegmentEnd(false);
        return msgList;
    }

    @Override
    public MsgList<M> finishFinal() {
        msgList.setSegmentEnd(false);
        return msgList;
    }

    @Override
    public void initAll(MsgList providedMsgList) {
        realInit(providedMsgList);
        msgList.setSegmentStart(true);
    }

    @Override
    public MsgList<M> finishFinalAll() {
        msgList.setSegmentEnd(true);
        return msgList;
    }

    @Override
    public int estimateAccumulatedStateByteSizePartial(I vertexIndex, M msg) throws HyracksDataException {
        return accumulatedSize + msg.sizeInBytes();
    }

    @Override
    public int estimateAccumulatedStateByteSizeFinal(I vertexIndex, MsgList partialAggregate)
            throws HyracksDataException {
        int size = accumulatedSize;
        for (int i = 0; i < partialAggregate.size(); i++) {
            size += ((M) partialAggregate.get(i)).sizeInBytes();
        }
        return size;
    }
}
