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

import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * The vertex context contains global states like superstep, the number of vertices, the number of edges
 */
public class VertexContext {

    private long superstep = 1;
    private long numVertices = 0;
    private long numEdges = 0;
    private TaskAttemptContext context;

    public VertexContext() {
    }

    public long getSuperstep() {
        return superstep;
    }

    public long getNumVertices() {
        return numVertices;
    }

    public long getNumEdges() {
        return numEdges;
    }

    public TaskAttemptContext getContext() {
        if (context == null) {
            throw new IllegalStateException("Job context has not been set.");
        }
        return context;
    }

    public void setSuperstep(long superstep) {
        this.superstep = superstep;
    }

    public void setContext(TaskAttemptContext context) {
        if (context == null) {
            throw new IllegalStateException("Do not set null job context.");
        }
        this.context = context;
    }

    public void setNumEdges(long numEdges) {
        this.numEdges = numEdges;
    }

    public void setNumVertices(long numVertices) {
        this.numVertices = numVertices;
    }

}
