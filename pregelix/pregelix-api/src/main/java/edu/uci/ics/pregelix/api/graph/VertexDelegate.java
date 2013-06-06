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

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.pregelix.api.util.FrameTupleUtils;

@SuppressWarnings("rawtypes")
class VertexDelegate<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable> {
    /** Vertex id */
    private I vertexId = null;
    /** Vertex value */
    private Vertex vertex = null;

    /** message tuple builder */
    private ArrayTupleBuilder message;
    private IFrameWriter msgWriter;
    private FrameTupleAppender appenderMsg;

    /** alive tuple builder */
    private ArrayTupleBuilder alive;
    private IFrameWriter aliveWriter;
    private FrameTupleAppender appenderAlive;

    /** the tuple for insert */
    private ArrayTupleBuilder insertTb;
    private IFrameWriter insertWriter;
    private FrameTupleAppender appenderInsert;

    /** the tuple for insert */
    private ArrayTupleBuilder deleteTb;
    private IFrameWriter deleteWriter;
    private FrameTupleAppender appenderDelete;

    /** message list */
    private MsgList dummyMessageList = new MsgList();
    /** whether alive message should be pushed out */
    private boolean pushAlive;

    public VertexDelegate(Vertex vertex) {
        this.vertex = vertex;
    }

    public void finishCompute() throws IOException {
        // package alive info
        if (pushAlive && !vertex.isHalted()) {
            alive.reset();
            DataOutput outputAlive = alive.getDataOutput();
            vertexId.write(outputAlive);
            alive.addFieldEndOffset();
            dummyMessageList.write(outputAlive);
            alive.addFieldEndOffset();
            FrameTupleUtils.flushTuple(appenderAlive, alive, aliveWriter);
        }
    }

    public final void sendMsg(I id, M msg) {
        if (msg == null) {
            throw new IllegalArgumentException("sendMsg: Cannot send null message to " + id);
        }

        /**
         * send out message along message channel
         */
        try {
            message.reset();
            DataOutput outputMsg = message.getDataOutput();
            id.write(outputMsg);
            message.addFieldEndOffset();
            msg.write(outputMsg);
            message.addFieldEndOffset();
            FrameTupleUtils.flushTuple(appenderMsg, message, msgWriter);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public final void setVertex(Vertex vertex) {
        this.vertex = vertex;
    }

    public final void setVertexId(I vertexId) {
        this.vertexId = vertexId;
    }

    public final void addVertex(I vertexId, Vertex vertex) {
        try {
            insertTb.reset();
            DataOutput outputInsert = insertTb.getDataOutput();
            vertexId.write(outputInsert);
            insertTb.addFieldEndOffset();
            vertex.write(outputInsert);
            insertTb.addFieldEndOffset();
            FrameTupleUtils.flushTuple(appenderInsert, insertTb, insertWriter);

            /**
             * push alive when necessary
             */
            if (pushAlive && !vertex.isHalted()) {
                alive.reset();
                DataOutput outputAlive = alive.getDataOutput();
                vertexId.write(outputAlive);
                alive.addFieldEndOffset();
                dummyMessageList.write(outputAlive);
                alive.addFieldEndOffset();
                FrameTupleUtils.flushTuple(appenderAlive, alive, aliveWriter);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public final void deleteVertex(I vertexId) {
        try {
            deleteTb.reset();
            DataOutput outputDelete = deleteTb.getDataOutput();
            vertexId.write(outputDelete);
            deleteTb.addFieldEndOffset();
            FrameTupleUtils.flushTuple(appenderDelete, deleteTb, deleteWriter);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public final void setOutputWriters(List<IFrameWriter> outputs) {
        msgWriter = outputs.get(0);
        insertWriter = outputs.get(1);
        deleteWriter = outputs.get(2);
        if (outputs.size() > 3) {
            aliveWriter = outputs.get(outputs.size() - 1);
            pushAlive = true;
        }
    }

    public final void setOutputAppenders(List<FrameTupleAppender> appenders) {
        appenderMsg = appenders.get(0);
        appenderInsert = appenders.get(1);
        appenderDelete = appenders.get(2);
        if (appenders.size() > 3) {
            appenderAlive = appenders.get(appenders.size() - 1);
        }
    }

    public final void setOutputTupleBuilders(List<ArrayTupleBuilder> tbs) {
        message = tbs.get(0);
        insertTb = tbs.get(1);
        deleteTb = tbs.get(2);
        if (tbs.size() > 3) {
            alive = tbs.get(tbs.size() - 1);
        }
    }
}
