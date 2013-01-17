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

import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexOutputFormat;

/**
 * This class represents a Pregelix job.
 */
public class PregelixJob extends Job {
    /** Vertex class - required */
    public static final String VERTEX_CLASS = "pregelix.vertexClass";
    /** VertexInputFormat class - required */
    public static final String VERTEX_INPUT_FORMAT_CLASS = "pregelix.vertexInputFormatClass";
    /** VertexOutputFormat class - optional */
    public static final String VERTEX_OUTPUT_FORMAT_CLASS = "pregelix.vertexOutputFormatClass";
    /** Vertex combiner class - optional */
    public static final String Message_COMBINER_CLASS = "pregelix.combinerClass";
    /** Global aggregator class - optional */
    public static final String GLOBAL_AGGREGATOR_CLASS = "pregelix.aggregatorClass";
    /** Vertex resolver class - optional */
    public static final String VERTEX_RESOLVER_CLASS = "pregelix.vertexResolverClass";
    /** Vertex index class */
    public static final String VERTEX_INDEX_CLASS = "pregelix.vertexIndexClass";
    /** Vertex value class */
    public static final String VERTEX_VALUE_CLASS = "pregelix.vertexValueClass";
    /** Edge value class */
    public static final String EDGE_VALUE_CLASS = "pregelix.edgeValueClass";
    /** Message value class */
    public static final String MESSAGE_VALUE_CLASS = "pregelix.messageValueClass";
    /** Partial combiner value class */
    public static final String PARTIAL_COMBINE_VALUE_CLASS = "pregelix.partialCombinedValueClass";
    /** Partial aggregate value class */
    public static final String PARTIAL_AGGREGATE_VALUE_CLASS = "pregelix.partialAggregateValueClass";
    /** Final aggregate value class */
    public static final String FINAL_AGGREGATE_VALUE_CLASS = "pregelix.finalAggregateValueClass";
    /** num of vertices */
    public static final String NUM_VERTICE = "pregelix.numVertices";
    /** num of edges */
    public static final String NUM_EDGES = "pregelix.numEdges";
    /** increase state length */
    public static final String INCREASE_STATE_LENGTH = "pregelix.incStateLength";
    /** job id */
    public static final String JOB_ID = "pregelix.jobid";

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
    final public void setMessageCombinerClass(Class<?> vertexCombinerClass) {
        getConfiguration().setClass(Message_COMBINER_CLASS, vertexCombinerClass, MessageCombiner.class);
    }

    /**
     * Set the global aggregator class (optional)
     * 
     * @param globalAggregatorClass
     *            Determines how messages are globally aggregated
     */
    final public void setGlobalAggregatorClass(Class<?> globalAggregatorClass) {
        getConfiguration().setClass(GLOBAL_AGGREGATOR_CLASS, globalAggregatorClass, GlobalAggregator.class);
    }

    /**
     * Set the job Id
     */
    final public void setJobId(String jobId) {
        getConfiguration().set(JOB_ID, jobId);
    }

    /**
     * Set whether the vertex state length can be dynamically increased
     * 
     * @param jobId
     */
    final public void setDynamicVertexValueSize(boolean incStateLengthDynamically) {
        getConfiguration().setBoolean(INCREASE_STATE_LENGTH, incStateLengthDynamically);
    }
}
