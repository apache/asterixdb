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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.JobStateUtils;

/**
 * User applications should all inherit {@link Vertex}, and implement their own
 * *compute* method.
 * 
 * @param <I>
 *            Vertex identifier type
 * @param <V>
 *            Vertex value type
 * @param <E>
 *            Edge value type
 * @param <M>
 *            Message value type
 */
@SuppressWarnings("rawtypes")
public abstract class Vertex<I extends WritableComparable, V extends Writable, E extends Writable, M extends WritableSizable>
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
    private static TaskAttemptContext context = null;
    /** a delegate for hyracks stuff */
    private VertexDelegate<I, V, E, M> delegate = new VertexDelegate<I, V, E, M>(this);
    /** this vertex is updated or not */
    private boolean updated = false;
    /** has outgoing messages */
    private boolean hasMessage = false;
    /** created new vertex */
    private boolean createdNewLiveVertex = false;
    /** terminate the partition */
    private boolean terminatePartition = false;

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
     * The key method that users need to implement to process
     * incoming messages in each superstep.
     * 1. In a superstep, this method can be called multiple times in a continuous manner for a single
     * vertex, each of which is to process a batch of messages. (Note that
     * this only happens for the case when the mssages for a single vertex
     * exceed one frame.)
     * 2. In each superstep, before any invocation of this method for a vertex,
     * open() is called; after all the invocations of this method for the vertex,
     * close is called.
     * 3. In each partition, the vertex Java object is reused
     * for all the vertice to be processed in the same partition. (The model
     * is the same as the key-value objects in hadoop map tasks.)
     * 
     * @param msgIterator
     *            an iterator of incoming messages
     */
    public abstract void compute(Iterator<M> msgIterator) throws Exception;

    /**
     * Add an edge for the vertex.
     * 
     * @param targetVertexId
     * @param edgeValue
     * @return successful or not
     */
    public final boolean addEdge(I targetVertexId, E edgeValue) {
        Edge<I, E> edge = this.allocateEdge();
        edge.setDestVertexId(targetVertexId);
        edge.setEdgeValue(edgeValue);
        destEdgeList.add(edge);
        updated = true;
        return true;
    }

    /**
     * Initialize a new vertex
     * 
     * @param vertexId
     * @param vertexValue
     * @param edges
     * @param messages
     */
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

    /**
     * reset a vertex object: clear its internal states
     */
    public void reset() {
        usedEdge = 0;
        usedMessage = 0;
        usedValue = 0;
        updated = false;
    }

    /**
     * Set the vertex id
     * 
     * @param vertexId
     */
    public final void setVertexId(I vertexId) {
        this.vertexId = vertexId;
        delegate.setVertexId(vertexId);
    }

    /**
     * Get the vertex id
     * 
     * @return vertex id
     */
    public final I getVertexId() {
        return vertexId;
    }

    /**
     * Get the vertex value
     * 
     * @return the vertex value
     */
    public final V getVertexValue() {
        return vertexValue;
    }

    /**
     * Set the vertex value
     * 
     * @param vertexValue
     */
    public final void setVertexValue(V vertexValue) {
        this.vertexValue = vertexValue;
        this.updated = true;
    }

    /***
     * Send a message to a specific vertex
     * 
     * @param id
     *            the receiver vertex id
     * @param msg
     *            the message
     */
    public final void sendMsg(I id, M msg) {
        if (msg == null) {
            throw new IllegalArgumentException("sendMsg: Cannot send null message to " + id);
        }
        delegate.sendMsg(id, msg);
        this.hasMessage = true;
    }

    /**
     * Send a message to all direct outgoing neighbors
     * 
     * @param msg
     *            the message
     */
    public final void sendMsgToAllEdges(M msg) {
        if (msg == null) {
            throw new IllegalArgumentException("sendMsgToAllEdges: Cannot send null message to all edges");
        }
        for (Edge<I, E> edge : destEdgeList) {
            sendMsg(edge.getDestVertexId(), msg);
        }
    }

    /**
     * Vote to halt. Once all vertex vote to halt and no more messages, a
     * Pregelix job will terminate.
     */
    public final void voteToHalt() {
        halt = true;
        updated = true;
    }

    /**
     * Activate a halted vertex such that it is alive again.
     */
    public final void activate() {
        halt = false;
        updated = true;
    }

    /**
     * @return the vertex is halted (true) or not (false)
     */
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
        long edgeMapSize = WritableUtils.readVLong(in);
        for (long i = 0; i < edgeMapSize; ++i) {
            Edge<I, E> edge = allocateEdge();
            edge.setConf(getContext().getConfiguration());
            edge.readFields(in);
            addEdge(edge);
        }
        msgList.clear();
        long msgListSize = WritableUtils.readVLong(in);
        for (long i = 0; i < msgListSize; ++i) {
            M msg = allocateMessage();
            msg.readFields(in);
            msgList.add(msg);
        }
        halt = in.readBoolean();
        updated = false;
        hasMessage = false;
        createdNewLiveVertex = false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vertexId.write(out);
        out.writeBoolean(vertexValue != null);
        if (vertexValue != null) {
            vertexValue.write(out);
        }
        WritableUtils.writeVLong(out, destEdgeList.size());
        for (Edge<I, E> edge : destEdgeList) {
            edge.write(out);
        }
        WritableUtils.writeVLong(out, msgList.size());
        for (M msg : msgList) {
            msg.write(out);
        }
        out.writeBoolean(halt);
    }

    /**
     * Get the list of incoming messages
     * 
     * @return the list of messages
     */
    public List<M> getMsgList() {
        return msgList;
    }

    /**
     * Get outgoing edge list
     * 
     * @return a list of outgoing edges
     */
    public List<Edge<I, E>> getEdges() {
        return this.destEdgeList;
    }

    @Override
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

    /**
     * Get the number of outgoing edges
     * 
     * @return the number of outging edges
     */
    public int getNumOutEdges() {
        return destEdgeList.size();
    }

    /**
     * Pregelix internal use only
     * 
     * @param writers
     */
    public void setOutputWriters(List<IFrameWriter> writers) {
        delegate.setOutputWriters(writers);
    }

    /**
     * Pregelix internal use only
     * 
     * @param writers
     */
    public void setOutputAppenders(List<FrameTupleAppender> appenders) {
        delegate.setOutputAppenders(appenders);
    }

    /**
     * Pregelix internal use only
     * 
     * @param writers
     */
    public void setOutputTupleBuilders(List<ArrayTupleBuilder> tbs) {
        delegate.setOutputTupleBuilders(tbs);
    }

    /**
     * Pregelix internal use only
     * 
     * @param writers
     */
    public void finishCompute() throws IOException {
        delegate.finishCompute();
    }

    /**
     * Pregelix internal use only
     */
    public boolean hasUpdate() {
        return this.updated;
    }

    /**
     * Pregelix internal use only
     */
    public boolean hasMessage() {
        return this.hasMessage;
    }

    /**
     * Pregelix internal use only
     */
    public boolean createdNewLiveVertex() {
        return this.createdNewLiveVertex;
    }

    /**
     * sort the edges
     */
    @SuppressWarnings("unchecked")
    public void sortEdges() {
        updated = true;
        Collections.sort(destEdgeList);
    }

    /**
     * Allocate a new edge from the edge pool
     */
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

    /**
     * Allocate a new message from the message pool
     */
    private M allocateMessage() {
        M message;
        if (usedMessage < msgPool.size()) {
            message = msgPool.get(usedMessage);
            usedMessage++;
        } else {
            message = BspUtils.<M> createMessageValue(getContext().getConfiguration());
            msgPool.add(message);
            usedMessage++;
        }
        return message;
    }

    /**
     * Set the global superstep for all the vertices (internal use)
     * 
     * @param superstep
     *            New superstep
     */
    public static final void setSuperstep(long superstep) {
        Vertex.superstep = superstep;
    }

    /**
     * Add an outgoing edge into the vertex
     * 
     * @param edge
     *            the edge to be added
     * @return true if the edge list changed as a result of this call
     */
    public boolean addEdge(Edge<I, E> edge) {
        edge.setConf(getContext().getConfiguration());
        updated = true;
        return destEdgeList.add(edge);
    }

    /**
     * remove an outgoing edge in the graph
     * 
     * @param edge
     *            the edge to be removed
     * @return true if the edge is in the edge list of the vertex
     */
    public boolean removeEdge(Edge<I, E> edge) {
        updated = true;
        return destEdgeList.remove(edge);
    }

    /**
     * Add a new vertex into the graph
     * 
     * @param vertexId
     *            the vertex id
     * @param vertex
     *            the vertex
     */
    public final void addVertex(I vertexId, Vertex vertex) {
        createdNewLiveVertex |= !vertex.isHalted();
        delegate.addVertex(vertexId, vertex);
    }

    /**
     * Delete a vertex from id
     * 
     * @param vertexId
     *            the vertex id
     */
    public final void deleteVertex(I vertexId) {
        delegate.deleteVertex(vertexId);
    }

    /**
     * Allocate a vertex value from the object pool
     * 
     * @return a vertex value instance
     */
    private V allocateValue() {
        V value;
        if (usedValue < valuePool.size()) {
            value = valuePool.get(usedValue);
            usedValue++;
        } else {
            value = BspUtils.<V> createVertexValue(getContext().getConfiguration());
            valuePool.add(value);
            usedValue++;
        }
        return value;
    }

    /**
     * Get the current global superstep number
     * 
     * @return the current superstep number
     */
    public static final long getSuperstep() {
        return superstep;
    }

    /**
     * Set the total number of vertices from the last superstep.
     * 
     * @param numVertices
     *            Aggregate vertices in the last superstep
     */
    public static final void setNumVertices(long numVertices) {
        Vertex.numVertices = numVertices;
    }

    /**
     * Get the number of vertexes in the graph
     * 
     * @return the number of vertexes in the graph
     */
    public static final long getNumVertices() {
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

    /**
     * Get the number of edges from this graph
     * 
     * @return the number of edges in the graph
     */
    public static final long getNumEdges() {
        return numEdges;
    }

    /**
     * Pregelix internal use only
     */
    public static final TaskAttemptContext getContext() {
        return context;
    }

    /**
     * Pregelix internal use only
     * 
     * @param context
     */
    public static final void setContext(TaskAttemptContext context) {
        Vertex.context = context;
    }

    @Override
    public int hashCode() {
        return vertexId.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        Vertex vertex = (Vertex) object;
        return vertexId.equals(vertex.getVertexId());
    }

    /**
     * called immediately before invocations of compute() on a vertex
     * Users can override this method to initiate the state for a vertex
     * before the compute() invocations
     */
    public void open() {

    }

    /**
     * called immediately after all the invocations of compute() on a vertex
     * Users can override this method to initiate the state for a vertex
     * before the compute() invocations
     */
    public void close() {

    }

    /**
     * Terminate the current partition where the current vertex stays in.
     * This will immediately take effect and the upcoming vertice in the
     * same partition cannot be processed.
     */
    protected final void terminatePartition() {
        voteToHalt();
        terminatePartition = true;
    }

    /**
     * Terminate the Pregelix job.
     * This will take effect only when the current iteration completed.
     * 
     * @throws Exception
     */
    protected void terminateJob() throws Exception {
        Configuration conf = getContext().getConfiguration();
        JobStateUtils.writeForceTerminationState(conf, BspUtils.getJobId(conf));
    }

    /***
     * @return true if the partition is terminated; false otherwise
     */
    public boolean isPartitionTerminated() {
        return terminatePartition;
    }

}
