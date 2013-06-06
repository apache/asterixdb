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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.pregelix.api.util.BspUtils;

/**
 * The Edge class, represent an outgoing edge inside an {@link Vertex} object.
 * 
 * @param <I>
 *            Vertex index
 * @param <E>
 *            Edge value
 */
@SuppressWarnings("rawtypes")
public class Edge<I extends WritableComparable, E extends Writable> implements Writable, Configurable, Comparable {
    /** Destination vertex id */
    private I destVertexId = null;
    /** Edge value */
    private E edgeValue = null;
    /** Configuration - Used to instantiate classes */
    private Configuration conf = null;
    /** Whether the edgeValue field is not null */
    private boolean hasEdgeValue = false;

    /**
     * Constructor for reflection
     */
    public Edge() {
    }

    /**
     * Create the edge with final values
     * 
     * @param destVertexId
     * @param edgeValue
     */
    public Edge(I destVertexId, E edgeValue) {
        this.destVertexId = destVertexId;
        this.edgeValue = edgeValue;
        if (edgeValue != null)
            hasEdgeValue = true;
    }

    /**
     * Get the destination vertex index of this edge
     * 
     * @return Destination vertex index of this edge
     */
    public I getDestVertexId() {
        return destVertexId;
    }

    /**
     * set the destination vertex id
     * 
     * @param destVertexId
     */
    public void setDestVertexId(I destVertexId) {
        this.destVertexId = destVertexId;
    }

    /**
     * Get the edge value of the edge
     * 
     * @return Edge value of this edge
     */
    public E getEdgeValue() {
        return edgeValue;
    }

    /**
     * set the edge of value
     * 
     * @param edgeValue
     */
    public void setEdgeValue(E edgeValue) {
        this.edgeValue = edgeValue;
        if (edgeValue != null)
            hasEdgeValue = true;
    }

    @Override
    public String toString() {
        return "(DestVertexIndex = " + destVertexId + ", edgeValue = " + edgeValue + ")";
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput input) throws IOException {
        if (destVertexId == null)
            destVertexId = (I) BspUtils.createVertexIndex(getConf());
        destVertexId.readFields(input);
        hasEdgeValue = input.readBoolean();
        if (hasEdgeValue) {
            if (edgeValue == null) {
                edgeValue = (E) BspUtils.createEdgeValue(getConf());
            }
            edgeValue.readFields(input);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        if (destVertexId == null) {
            throw new IllegalStateException("write: Null destination vertex index");
        }
        destVertexId.write(output);
        output.writeBoolean(hasEdgeValue);
        if (hasEdgeValue) {
            edgeValue.write(output);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public boolean equals(Edge<I, E> edge) {
        return this.destVertexId.equals(edge.getDestVertexId());
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Object o) {
        Edge<I, E> edge = (Edge<I, E>) o;
        return destVertexId.compareTo(edge.getDestVertexId());
    }
}
