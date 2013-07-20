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

package edu.uci.ics.pregelix.api.graph;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.io.WritableSizable;

/**
 * This is the abstract class to implement for combining of messages that are sent to the same vertex.
 * </p>
 * This is similar to the concept of Combiner in Hadoop. The combining of messages in a distributed
 * cluster include two phase:
 * 1. a local phase which combines messages sent from a single machine and produces
 * the partially combined message;
 * 2. a final phase which combines messages at each receiver machine after the repartitioning (shuffling)
 * and produces the final combined message
 * 
 * @param <I extends Writable> vertex identifier
 * @param <M extends Writable> message body type
 * @param <P extends Writable>
 *        the type of the partially combined messages
 */
@SuppressWarnings("rawtypes")
public abstract class MessageCombiner<I extends WritableComparable, M extends WritableSizable, P extends Writable> {

    /**
     * initialize combiner
     * 
     * @param providedMsgList
     *            the provided msg list for user implementation to update, which *should* be returned
     *            by the finishFinal() method
     */
    public abstract void init(MsgList providedMsgList);

    /**
     * step call for local combiner
     * 
     * @param vertexIndex
     *            the receiver vertex identifier
     * @param msg
     *            a single message body
     * @throws HyracksDataException
     */
    public abstract void stepPartial(I vertexIndex, M msg) throws HyracksDataException;

    /**
     * step call for global combiner
     * 
     * @param vertexIndex
     *            the receiver vertex identifier
     * @param partialAggregate
     *            the partial aggregate value
     * @throws HyracksDataException
     */
    public abstract void stepFinal(I vertexIndex, P partialAggregate) throws HyracksDataException;

    /**
     * finish partial combiner
     * 
     * @return the intermediate combined message of type P
     */
    public abstract P finishPartial();

    /**
     * finish final combiner
     * 
     * @return the final message List
     */
    public abstract MsgList<M> finishFinal();

    /**
     * init the combiner for all segmented bags for one key
     * 
     * @return the final message List
     */
    public void initAll(MsgList providedMsgList) {
        init(providedMsgList);
    }

    /**
     * finish final combiner for all segmented bags for one key
     * 
     * @return the final message List
     */
    public MsgList<M> finishFinalAll() {
        return finishFinal();
    }

    /**
     * @return the accumulated byte size
     */
    public int estimateAccumulatedStateByteSizePartial(I vertexIndex, M msg) throws HyracksDataException {
        return 0;
    }

    /**
     * @return the accumulated byte size
     */
    public int estimateAccumulatedStateByteSizeFinal(I vertexIndex, P partialAggregate) throws HyracksDataException {
        return 0;
    }
}
