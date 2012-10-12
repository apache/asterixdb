/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.pregelix.api.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.graph.VertexCombiner;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexOutputFormat;

/**
 * Limits the functions that can be called by the user. Job is too flexible for
 * our needs. For instance, our job should not have any reduce tasks.
 */
public class PregelixJob extends Job {
    /** Vertex class - required */
    public static final String VERTEX_CLASS = "giraph.vertexClass";
    /** VertexInputFormat class - required */
    public static final String VERTEX_INPUT_FORMAT_CLASS = "giraph.vertexInputFormatClass";
    /** VertexOutputFormat class - optional */
    public static final String VERTEX_OUTPUT_FORMAT_CLASS = "giraph.vertexOutputFormatClass";
    /** Vertex combiner class - optional */
    public static final String VERTEX_COMBINER_CLASS = "giraph.combinerClass";
    /** Vertex resolver class - optional */
    public static final String VERTEX_RESOLVER_CLASS = "giraph.vertexResolverClass";
    /** Vertex index class */
    public static final String VERTEX_INDEX_CLASS = "giraph.vertexIndexClass";
    /** Vertex value class */
    public static final String VERTEX_VALUE_CLASS = "giraph.vertexValueClass";
    /** Edge value class */
    public static final String EDGE_VALUE_CLASS = "giraph.edgeValueClass";
    /** Message value class */
    public static final String MESSAGE_VALUE_CLASS = "giraph.messageValueClass";
    /** Aggregator class */
    public static final String AGGREGATOR_NAME = "giraph.aggregatorClass";
    /** num of vertices */
    public static final String NUM_VERTICE = "giraph.numVertices";
    /** num of edges */
    public static final String NUM_EDGES = "giraph.numEdges";

    /**
     * Constructor that will instantiate the configuration
     * 
     * @param jobName
     *            User-defined job name
     * @throws IOException
     */
    public PregelixJob(String jobName) throws IOException {
        super(new Configuration(), jobName);
    }

    /**
     * Constructor.
     * 
     * @param conf
     *            User-defined configuration
     * @param jobName
     *            User-defined job name
     * @throws IOException
     */
    public PregelixJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
    }

    /**
     * Set the vertex class (required)
     * 
     * @param vertexClass
     *            Runs vertex computation
     */
    final public void setVertexClass(Class<?> vertexClass) {
        getConfiguration().setClass(VERTEX_CLASS, vertexClass, Vertex.class);
    }

    /**
     * Set the vertex input format class (required)
     * 
     * @param vertexInputFormatClass
     *            Determines how graph is input
     */
    final public void setVertexInputFormatClass(Class<?> vertexInputFormatClass) {
        getConfiguration().setClass(VERTEX_INPUT_FORMAT_CLASS, vertexInputFormatClass, VertexInputFormat.class);
    }

    /**
     * Set the vertex output format class (optional)
     * 
     * @param vertexOutputFormatClass
     *            Determines how graph is output
     */
    final public void setVertexOutputFormatClass(Class<?> vertexOutputFormatClass) {
        getConfiguration().setClass(VERTEX_OUTPUT_FORMAT_CLASS, vertexOutputFormatClass, VertexOutputFormat.class);
    }

    /**
     * Set the vertex combiner class (optional)
     * 
     * @param vertexCombinerClass
     *            Determines how vertex messages are combined
     */
    final public void setVertexCombinerClass(Class<?> vertexCombinerClass) {
        getConfiguration().setClass(VERTEX_COMBINER_CLASS, vertexCombinerClass, VertexCombiner.class);
    }
}
