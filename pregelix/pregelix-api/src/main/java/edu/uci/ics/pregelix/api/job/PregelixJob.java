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

package edu.uci.ics.pregelix.api.job;

import java.io.IOException;
import java.lang.reflect.Modifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.NormalizedKeyComputer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.graph.VertexPartitioner;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.VertexOutputFormat;
import edu.uci.ics.pregelix.api.util.GlobalEdgeCountAggregator;
import edu.uci.ics.pregelix.api.util.GlobalVertexCountAggregator;
import edu.uci.ics.pregelix.api.util.HadoopCountersAggregator;
import edu.uci.ics.pregelix.api.util.HadoopCountersGlobalAggregateHook;

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
    /** The normalized key computer class */
    public static final String NMK_COMPUTER_CLASS = "pregelix.nmkComputerClass";
    /** The partitioner class */
    public static final String PARTITIONER_CLASS = "pregelix.partitionerClass";
    /** num of vertices */
    public static final String NUM_VERTICE = "pregelix.numVertices";
    /** num of edges */
    public static final String NUM_EDGES = "pregelix.numEdges";
    /** increase state length */
    public static final String INCREASE_STATE_LENGTH = "pregelix.incStateLength";
    /** job id */
    public static final String JOB_ID = "pregelix.jobid";
    /** frame size */
    public static final String FRAME_SIZE = "pregelix.framesize";
    /** update intensive */
    public static final String UPDATE_INTENSIVE = "pregelix.updateIntensive";
    /** the check point hook */
    public static final String CKP_CLASS = "pregelix.checkpointHook";
    /** the check point hook */
    public static final String RECOVERY_COUNT = "pregelix.recoveryCount";
    /** the checkpoint interval */
    public static final String CKP_INTERVAL = "pregelix.ckpinterval";
    /** the dynamic optimization */
    public static final String DYNAMIC_OPTIMIZATION = "pregelix.dynamicopt";
    /** the iteration complete reporter hook */
    public static final String ITERATION_COMPLETE_CLASS = "pregelix.iterationCompleteReporter";
    /** comma */
    public static final String COMMA_STR = ",";
    /** period */
    public static final String PERIOD_STR = ".";
    /** the names of the aggregator classes active for all vertex types */
    public static final String[] DEFAULT_GLOBAL_AGGREGATOR_CLASSES = { GlobalVertexCountAggregator.class.getName(),
            GlobalEdgeCountAggregator.class.getName() };
    /** The name of an optional class that aggregates all Vertexes into mapreduce.Counters */
    public static final String COUNTERS_AGGREGATOR_CLASS = "pregelix.aggregatedCountersClass";
    /** the group-by algorithm */
    public static final String GROUPING_ALGORITHM = "pregelix.groupalg";
    /** the memory assigned to group-by */
    public static final String GROUPING_MEM = "pregelix.groupmem";
    /** the memory assigned for the sort operator */
    public static final String SORT_MEM = "pregelix.sortmem";
    /** the number of workers */
    public static final String NUM_WORKERS = "pregelix.numworkers";
    /** the application allows to skip combiner key during aggregations */
    public static final String SKIP_COMBINER_KEY = "pregelix.skipCombinerKey";
    /** the merge connector */
    public static final String MERGE_CONNECTOR = "pregelix.merge";
    /** the maximum allowed iteration */
    public static final String MAX_ITERATION="pregelix.maxiteration";

    /**
     * Construct a Pregelix job from an existing configuration
     * 
     * @param conf
     * @throws IOException
     */
    public PregelixJob(Configuration conf) throws IOException {
        super(conf);
    }

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
    final public void addGlobalAggregatorClass(Class<?> globalAggregatorClass) {
        String aggStr = globalAggregatorClass.getName();
        String classes = getConfiguration().get(GLOBAL_AGGREGATOR_CLASS);
        conf.set(GLOBAL_AGGREGATOR_CLASS, classes == null ? aggStr : classes + COMMA_STR + aggStr);
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

    /**
     * Set whether the vertex state length is fixed
     * 
     * @param jobId
     */
    final public void setFixedVertexValueSize(boolean fixedSize) {
        getConfiguration().setBoolean(INCREASE_STATE_LENGTH, !fixedSize);
    }

    /**
     * Set the frame size for a job
     * 
     * @param frameSize
     *            the desired frame size
     */
    final public void setFrameSize(int frameSize) {
        getConfiguration().setInt(FRAME_SIZE, frameSize);
    }

    /**
     * Set the normalized key computer class
     * 
     * @param nkcClass
     */
    final public void setNoramlizedKeyComputerClass(Class<?> nkcClass) {
        getConfiguration().setClass(NMK_COMPUTER_CLASS, nkcClass, NormalizedKeyComputer.class);
    }

    /**
     * Set the vertex partitioner class
     * 
     * @param partitionerClass
     */
    final public void setVertexPartitionerClass(Class<?> partitionerClass) {
        getConfiguration().setClass(PARTITIONER_CLASS, partitionerClass, VertexPartitioner.class);
    }

    /**
     * Indicate if the job needs to do a lot of graph mutations or variable size updates
     * 
     * @param updateHeavyFlag
     */
    final public void setLSMStorage(boolean variableSizedUpdateHeavyFlag) {
        getConfiguration().setBoolean(UPDATE_INTENSIVE, variableSizedUpdateHeavyFlag);
    }

    /**
     * Users can provide an ICheckpointHook implementation to specify when to do checkpoint
     * 
     * @param ckpClass
     */
    final public void setCheckpointHook(Class<?> ckpClass) {
        getConfiguration().setClass(CKP_CLASS, ckpClass, ICheckpointHook.class);
    }

    /**
     * Users can provide an ICheckpointHook implementation to specify when to do checkpoint
     * 
     * @param ckpClass
     */
    final public void setRecoveryCount(int recoveryCount) {
        getConfiguration().setInt(RECOVERY_COUNT, recoveryCount);
    }

    /**
     * Users can set the interval of checkpointing
     * 
     * @param ckpInterval
     */
    final public void setCheckpointingInterval(int ckpInterval) {
        getConfiguration().setInt(CKP_INTERVAL, ckpInterval);
    }

    /**
     * Users can provide an IIterationCompleteReporterHook implementation to perform actions
     * at the end of each iteration
     * 
     * @param reporterClass
     */
    final public void setIterationCompleteReporterHook(Class<? extends IIterationCompleteReporterHook> reporterClass) {
        getConfiguration().setClass(ITERATION_COMPLETE_CLASS, reporterClass, IIterationCompleteReporterHook.class);
    }

    /**
     * Indicate if dynamic optimization is enabled
     * 
     * @param dynamicOpt
     */
    final public void setEnableDynamicOptimization(boolean dynamicOpt) {
        getConfiguration().setBoolean(DYNAMIC_OPTIMIZATION, dynamicOpt);
    }

    /**
     * Set the counter aggregator class
     * 
     * @param aggClass
     */
    final public void setCounterAggregatorClass(Class<? extends HadoopCountersAggregator<?, ?, ?, ?, ?>> aggClass) {
        if (Modifier.isAbstract(aggClass.getModifiers())) {
            throw new IllegalArgumentException("Aggregate class must be a concrete class, not an abstract one! (was "
                    + aggClass.getName() + ")");
        }
        getConfiguration().setClass(COUNTERS_AGGREGATOR_CLASS, aggClass, HadoopCountersAggregator.class);
        addGlobalAggregatorClass(aggClass);
        setIterationCompleteReporterHook(HadoopCountersGlobalAggregateHook.class);
    }

    /**
     * Set the group-by algorithm: sort-true or hash-false
     * 
     * @param sortOrHash
     */
    final public void setGroupByAlgorithm(boolean sortOrHash) {
        getConfiguration().setBoolean(GROUPING_ALGORITHM, sortOrHash);
    }

    /**
     * Set the memory buget for group-by operators (only hash-based)
     * 
     * @param numberOfPages
     */
    final public void setGroupByMemoryLimit(int numberOfPages) {
        getConfiguration().setInt(GROUPING_MEM, numberOfPages);
    }

    /**
     * Set the memory buget for sort operators (only hash-based)
     * 
     * @param numberOfPages
     */
    final public void setSortMemoryLimit(int numberOfPages) {
        getConfiguration().setInt(SORT_MEM, numberOfPages);
    }

    /**
     * Set the number of workers
     * 
     * @param numWorkers
     */
    final public void setNumWorkers(int numWorkers) {
        getConfiguration().setInt(NUM_WORKERS, numWorkers);
    }

    /**
     * Whether an application allows to skip the combiner key during message combination,
     * this is a performance improvement tip.
     * By default, the key is not skipped
     * 
     * @param skip
     *            true to skip; otherwise, not.
     */
    final public void setSkipCombinerKey(boolean skip) {
        getConfiguration().setBoolean(SKIP_COMBINER_KEY, skip);
    }
    
    /**
     * Whether to use merge connector
     * 
     * @param merge
     */
    final public void setMergeConnector(boolean merge){
        getConfiguration().setBoolean(MERGE_CONNECTOR, merge);
    }
    
    /***
     * Set the maximum allowed iteration
     * 
     * @param iteration
     */
    final public void setMaxIteration(int iteration){
        getConfiguration().setInt(MAX_ITERATION, iteration);
    }

    @Override
    public String toString() {
        return getJobName();
    }
}
