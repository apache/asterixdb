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
    public void stepPartial(I vertexIndex, M msg) throws HyracksDataException {
        msgList.addElement(msg);
    }

    @Override
    public void stepFinal(I vertexIndex, MsgList partialAggregate) throws HyracksDataException {
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
