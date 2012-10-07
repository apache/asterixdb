/*
 * Licensed to Yahoo! under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Yahoo! licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.pregelix.api.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.pregelix.api.delegate.VertexDelegate;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.SerDeUtils;

/**
 * User applications should all subclass {@link Vertex}. Package access should
 * prevent users from accessing internal methods.
 * 
 * @param <I>
 *            Vertex index value
 * @param <V>
 *            Vertex value
 * @param <E>
 *            Edge value
 * @param <M>
 *            Message value
 */
@SuppressWarnings("rawtypes")
public abstract class Vertex<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
        implements Writable {
    private static long superstep = 0;
    /** Class-wide number of vertices */
    private static long numVertices = -1;
    /** Class-wide number of edges */
    private static long numEdges = -1;
    /** Vertex id */
    private I vertexId = null;
    /** Vertex value */
    private V vertexValue = null;
    /** Map of destination vertices and their edge values */
    private final List<Edge<I, E>> destEdgeList = new ArrayList<Edge<I, E>>();
    /** If true, do not do anymore computation on this vertex. */
    boolean halt = false;
    /** List of incoming messages from the previous superstep */
    private final List<M> msgList = new ArrayList<M>();
    /** map context */
    private static Mapper.Context context = null;
    /** a delegate for hyracks stuff */
    private VertexDelegate<I, V, E, M> delegate = new VertexDelegate<I, V, E, M>(this);
    /** this vertex is updated or not */
    private boolean updated = false;
    /** has outgoing messages */
    private boolean hasMessage = false;

    /**
     * use object pool for re-using objects
     */
    private List<Edge<I, E>> edgePool = new ArrayList<Edge<I, E>>();
    private List<M> msgPool = new ArrayList<M>();
    private List<V> valuePool = new ArrayList<V>();
    private int usedEdge = 0;
    private int usedMessage = 0;
    private int usedValue = 0;

    /**
     * The key method that users need to implement
     * 
     * @param msgIterator
     */
    public abstract void compute(Iterator<M> msgIterator);

    public final boolean addEdge(I targetVertexId, E edgeValue) {
        Edge<I, E> edge = this.allocateEdge();
        edge.setDestVertexId(targetVertexId);
        edge.setEdgeValue(edgeValue);
        destEdgeList.add(edge);
        return true;
    }

    public void initialize(I vertexId, V vertexValue, Map<I, E> edges, List<M> messages) {
        if (vertexId != null) {
            setVertexId(vertexId);
        }
        if (vertexValue != null) {
            setVertexValue(vertexValue);
        }
        destEdgeList.clear();
        if (edges != null && !edges.isEmpty()) {
            for (Map.Entry<I, E> entry : edges.entrySet()) {
                destEdgeList.add(new Edge<I, E>(entry.getKey(), entry.getValue()));
            }
        }
        if (messages != null && !messages.isEmpty()) {
            msgList.addAll(messages);
        }
    }

    public void reset() {
        usedEdge = 0;
        usedMessage = 0;
        usedValue = 0;
    }

    private Edge<I, E> allocateEdge() {
        Edge<I, E> edge;
        if (usedEdge < edgePool.size()) {
            edge = edgePool.get(usedEdge);
            usedEdge++;
        } else {
            edge = new Edge<I, E>();
            edgePool.add(edge);
            usedEdge++;
        }
        return edge;
    }

    private M allocateMessage() {
        M message;
        if (usedMessage < msgPool.size()) {
            message = msgPool.get(usedEdge);
            usedMessage++;
        } else {
            message = BspUtils.<M> createMessageValue(getContext().getConfiguration());
            msgPool.add(message);
            usedMessage++;
        }
        return message;
    }

    private V allocateValue() {
        V value;
        if (usedValue < valuePool.size()) {
            value = valuePool.get(usedEdge);
            usedValue++;
        } else {
            value = BspUtils.<V> createVertexValue(getContext().getConfiguration());
            valuePool.add(value);
            usedValue++;
        }
        return value;
    }

    public final void setVertexId(I vertexId) {
        this.vertexId = vertexId;
        delegate.setVertexId(vertexId);
    }

    public final I getVertexId() {
        return vertexId;
    }

    /**
     * Set the global superstep folr all the vertices (internal use)
     * 
     * @param superstep
     *            New superstep
     */
    public static void setSuperstep(long superstep) {
        Vertex.superstep = superstep;
    }

    public static long getCurrentSuperstep() {
        return superstep;
    }

    public final long getSuperstep() {
        return superstep;
    }

    public final V getVertexValue() {
        return vertexValue;
    }

    public final void setVertexValue(V vertexValue) {
        this.vertexValue = vertexValue;
        this.updated = true;
    }

    /**
     * Set the total number of vertices from the last superstep.
     * 
     * @param numVertices
     *            Aggregate vertices in the last superstep
     */
    public static void setNumVertices(long numVertices) {
        Vertex.numVertices = numVertices;
    }

    public final long getNumVertices() {
        return numVertices;
    }

    /**
     * Set the total number of edges from the last superstep.
     * 
     * @param numEdges
     *            Aggregate edges in the last superstep
     */
    public static void setNumEdges(long numEdges) {
        Vertex.numEdges = numEdges;
    }

    public final long getNumEdges() {
        return numEdges;
    }

    public final void sendMsg(I id, M msg) {
        if (msg == null) {
            throw new IllegalArgumentException("sendMsg: Cannot send null message to " + id);
        }
        delegate.sendMsg(id, msg);
        this.hasMessage = true;
    }

    public final void sendMsgToAllEdges(M msg) {
        if (msg == null) {
            throw new IllegalArgumentException("sendMsgToAllEdges: Cannot send null message to all edges");
        }
        for (Edge<I, E> edge : destEdgeList) {
            sendMsg(edge.getDestVertexId(), msg);
        }
    }

    public final void voteToHalt() {
        halt = true;
    }

    public final boolean isHalted() {
        return halt;
    }

    @Override
    final public void readFields(DataInput in) throws IOException {
        reset();
        if (vertexId == null)
            vertexId = BspUtils.<I> createVertexIndex(getContext().getConfiguration());
        vertexId.readFields(in);
        delegate.setVertexId(vertexId);
        boolean hasVertexValue = in.readBoolean();
        if (hasVertexValue) {
            vertexValue = allocateValue();
            vertexValue.readFields(in);
            delegate.setVertex(this);
        }
        destEdgeList.clear();
        long edgeMapSize = SerDeUtils.readVLong(in);
        for (long i = 0; i < edgeMapSize; ++i) {
            Edge<I, E> edge = allocateEdge();
            edge.setConf(getContext().getConfiguration());
            edge.readFields(in);
            addEdge(edge);
        }
        msgList.clear();
        long msgListSize = SerDeUtils.readVLong(in);
        for (long i = 0; i < msgListSize; ++i) {
            M msg = allocateMessage();
            msg.readFields(in);
            msgList.add(msg);
        }
        halt = in.readBoolean();
        updated = false;
        hasMessage = false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vertexId.write(out);
        out.writeBoolean(vertexValue != null);
        if (vertexValue != null) {
            vertexValue.write(out);
        }
        SerDeUtils.writeVLong(out, destEdgeList.size());
        for (Edge<I, E> edge : destEdgeList) {
            edge.write(out);
        }
        SerDeUtils.writeVLong(out, msgList.size());
        for (M msg : msgList) {
            msg.write(out);
        }
        out.writeBoolean(halt);
    }

    private boolean addEdge(Edge<I, E> edge) {
        edge.setConf(getContext().getConfiguration());
        destEdgeList.add(edge);
        return true;
    }

    public List<M> getMsgList() {
        return msgList;
    }

    public List<Edge<I, E>> getEdges() {
        return this.destEdgeList;
    }

    public final Mapper<?, ?, ?, ?>.Context getContext() {
        return context;
    }

    public final static void setContext(Mapper<?, ?, ?, ?>.Context context) {
        Vertex.context = context;
    }

    @SuppressWarnings("unchecked")
    public String toString() {
        Collections.sort(destEdgeList);
        StringBuffer edgeBuffer = new StringBuffer();
        edgeBuffer.append("(");
        for (Edge<I, E> edge : destEdgeList) {
            edgeBuffer.append(edge.getDestVertexId()).append(",");
        }
        edgeBuffer.append(")");
        return "Vertex(id=" + getVertexId() + ",value=" + getVertexValue() + ", edges=" + edgeBuffer + ")";
    }

    public void setOutputWriters(List<IFrameWriter> writers) {
        delegate.setOutputWriters(writers);
    }

    public void setOutputAppenders(List<FrameTupleAppender> appenders) {
        delegate.setOutputAppenders(appenders);
    }

    public void setOutputTupleBuilders(List<ArrayTupleBuilder> tbs) {
        delegate.setOutputTupleBuilders(tbs);
    }

    public void finishCompute() throws IOException {
        delegate.finishCompute();
    }

    public boolean hasUpdate() {
        return this.updated;
    }

    public boolean hasMessage() {
        return this.hasMessage;
    }

    public int getNumOutEdges() {
        return destEdgeList.size();
    }

    @SuppressWarnings("unchecked")
    public void sortEdges() {
        Collections.sort((List) destEdgeList);
    }

}
