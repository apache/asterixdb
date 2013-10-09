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

package edu.uci.ics.pregelix.core.jobgen;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.Algorithm;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs2.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexDropOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;
import edu.uci.ics.hyracks.storage.common.file.TransientLocalResourceFactoryProvider;
import edu.uci.ics.pregelix.api.graph.GlobalAggregator;
import edu.uci.ics.pregelix.api.graph.MessageCombiner;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.graph.VertexPartitioner;
import edu.uci.ics.pregelix.api.io.VertexInputFormat;
import edu.uci.ics.pregelix.api.io.internal.InternalVertexInputFormat;
import edu.uci.ics.pregelix.api.io.internal.InternalVertexOutputFormat;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.api.util.ReflectionUtils;
import edu.uci.ics.pregelix.core.base.IJobGen;
import edu.uci.ics.pregelix.core.data.TypeTraits;
import edu.uci.ics.pregelix.core.hadoop.config.ConfigurationFactory;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.runtime.touchpoint.RawBinaryComparatorFactory;
import edu.uci.ics.pregelix.core.runtime.touchpoint.RawNormalizedKeyComputerFactory;
import edu.uci.ics.pregelix.core.util.DataflowUtils;
import edu.uci.ics.pregelix.dataflow.ClearStateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.EmptySinkOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.EmptyTupleSourceOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.HDFSFileWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.KeyValueParserFactory;
import edu.uci.ics.pregelix.dataflow.MaterializingReadOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.MaterializingWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.VertexFileScanOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.VertexFileWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.VertexWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.RuntimeHookOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.bootstrap.IndexLifeCycleManagerProvider;
import edu.uci.ics.pregelix.runtime.bootstrap.StorageManagerInterface;
import edu.uci.ics.pregelix.runtime.bootstrap.VirtualBufferCacheProvider;
import edu.uci.ics.pregelix.runtime.touchpoint.RecoveryRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.RuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.VertexIdPartitionComputerFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.VertexPartitionComputerFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.WritableSerializerDeserializerFactory;

public abstract class JobGen implements IJobGen {
    private static final Logger LOGGER = Logger.getLogger(JobGen.class.getName());
    protected static final int MB = 1048576;
    protected static final float DEFAULT_BTREE_FILL_FACTOR = 1.00f;
    protected static final int tableSize = 10485767;
    protected static final String PRIMARY_INDEX = "primary";
    protected Configuration conf;
    protected PregelixJob pregelixJob;
    protected IIndexLifecycleManagerProvider lcManagerProvider = IndexLifeCycleManagerProvider.INSTANCE;
    protected IStorageManagerInterface storageManagerInterface = StorageManagerInterface.INSTANCE;
    protected String jobId = new UUID(System.currentTimeMillis(), System.nanoTime()).toString();
    protected int frameSize = ClusterConfig.getFrameSize();
    protected int maxFrameNumber = (int) (((long) 32 * MB) / frameSize);

    private static final Map<String, String> MERGE_POLICY_PROPERTIES;
    static {
        MERGE_POLICY_PROPERTIES = new HashMap<String, String>();
        MERGE_POLICY_PROPERTIES.put("num-components", "3");
    }

    protected static final String SECONDARY_INDEX_ODD = "secondary1";
    protected static final String SECONDARY_INDEX_EVEN = "secondary2";

    public JobGen(PregelixJob job) {
        init(job);
    }

    private void init(PregelixJob job) {
        conf = job.getConfiguration();
        pregelixJob = job;
        initJobConfiguration();
        job.setJobId(jobId);
        // set the frame size to be the one user specified if the user did specify.
        int specifiedFrameSize = BspUtils.getFrameSize(job.getConfiguration());
        if (specifiedFrameSize > 0) {
            frameSize = specifiedFrameSize;
            maxFrameNumber = (int) (((long) 32 * MB) / frameSize);
        }
        if (maxFrameNumber <= 0) {
            maxFrameNumber = 1;
        }
    }

    public void reset(PregelixJob job) {
        init(job);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void initJobConfiguration() {
        Class vertexClass = conf.getClass(PregelixJob.VERTEX_CLASS, Vertex.class);
        List<Type> parameterTypes = ReflectionUtils.getTypeArguments(Vertex.class, vertexClass);
        Type vertexIndexType = parameterTypes.get(0);
        Type vertexValueType = parameterTypes.get(1);
        Type edgeValueType = parameterTypes.get(2);
        Type messageValueType = parameterTypes.get(3);
        conf.setClass(PregelixJob.VERTEX_INDEX_CLASS, (Class<?>) vertexIndexType, WritableComparable.class);
        conf.setClass(PregelixJob.VERTEX_VALUE_CLASS, (Class<?>) vertexValueType, Writable.class);
        conf.setClass(PregelixJob.EDGE_VALUE_CLASS, (Class<?>) edgeValueType, Writable.class);
        conf.setClass(PregelixJob.MESSAGE_VALUE_CLASS, (Class<?>) messageValueType, Writable.class);

        Class aggregatorClass = BspUtils.getGlobalAggregatorClass(conf);
        if (!aggregatorClass.equals(GlobalAggregator.class)) {
            List<Type> argTypes = ReflectionUtils.getTypeArguments(GlobalAggregator.class, aggregatorClass);
            Type partialAggregateValueType = argTypes.get(4);
            conf.setClass(PregelixJob.PARTIAL_AGGREGATE_VALUE_CLASS, (Class<?>) partialAggregateValueType,
                    Writable.class);
            Type finalAggregateValueType = argTypes.get(5);
            conf.setClass(PregelixJob.FINAL_AGGREGATE_VALUE_CLASS, (Class<?>) finalAggregateValueType, Writable.class);
        }

        Class combinerClass = BspUtils.getMessageCombinerClass(conf);
        if (!combinerClass.equals(MessageCombiner.class)) {
            List<Type> argTypes = ReflectionUtils.getTypeArguments(MessageCombiner.class, combinerClass);
            Type partialCombineValueType = argTypes.get(2);
            conf.setClass(PregelixJob.PARTIAL_COMBINE_VALUE_CLASS, (Class<?>) partialCombineValueType, Writable.class);
        }
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    public JobSpecification generateCreatingJob() throws HyracksException {
        JobSpecification spec = new JobSpecification();
        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;

        int[] keyFields = new int[1];
        keyFields[0] = 0;
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);
        TreeIndexCreateOperatorDescriptor btreeCreate = new TreeIndexCreateOperatorDescriptor(spec,
                storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories,
                keyFields, getIndexDataflowHelperFactory(), new TransientLocalResourceFactoryProvider(),
                NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, btreeCreate);
        spec.setFrameSize(frameSize);
        return spec;
    }

    @Override
    public JobSpecification generateJob(int iteration) throws HyracksException {
        if (iteration <= 0)
            throw new IllegalStateException("iteration number cannot be less than 1");
        if (iteration == 1)
            return generateFirstIteration(iteration);
        else
            return generateNonFirstIteration(iteration);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public JobSpecification scanSortPrintGraph(String nodeName, String path) throws HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        Class<? extends Writable> vertexClass = BspUtils.getVertexClass(conf);
        int maxFrameLimit = (int) (((long) 512 * MB) / frameSize);
        JobSpecification spec = new JobSpecification();
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);

        /**
         * the graph file scan operator and use count constraint first, will use
         * absolute constraint later
         */
        VertexInputFormat inputFormat = BspUtils.createVertexInputFormat(conf);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        try {
            splits = inputFormat.getSplits(pregelixJob, fileSplitProvider.getFileSplits().length);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), vertexClass.getName());
        IConfigurationFactory confFactory = new ConfigurationFactory(conf);
        String[] readSchedule = ClusterConfig.getHdfsScheduler().getLocationConstraints(splits);
        VertexFileScanOperatorDescriptor scanner = new VertexFileScanOperatorDescriptor(spec, recordDescriptor, splits,
                readSchedule, confFactory);
        ClusterConfig.setLocationConstraint(spec, scanner);

        /**
         * construct sort operator
         */
        int[] sortFields = new int[1];
        sortFields[0] = 0;
        INormalizedKeyComputerFactory nkmFactory = JobGenUtil.getINormalizedKeyComputerFactory(conf);
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, maxFrameLimit, sortFields,
                nkmFactory, comparatorFactories, recordDescriptor);
        ClusterConfig.setLocationConstraint(spec, sorter);

        /**
         * construct write file operator
         */
        FileSplit resultFile = new FileSplit(nodeName, new FileReference(new File(path)));
        FileSplit[] results = new FileSplit[1];
        results[0] = resultFile;
        IFileSplitProvider resultFileSplitProvider = new ConstantFileSplitProvider(results);
        IRuntimeHookFactory preHookFactory = new RuntimeHookFactory(confFactory);
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                vertexIdClass.getName(), vertexClass.getName());
        VertexWriteOperatorDescriptor writer = new VertexWriteOperatorDescriptor(spec, inputRdFactory,
                resultFileSplitProvider, preHookFactory, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, writer, new String[] { "nc1" });
        PartitionConstraintHelper.addPartitionCountConstraint(spec, writer, 1);

        /**
         * connect operator descriptors
         */
        ITuplePartitionComputerFactory hashPartitionComputerFactory = getVertexPartitionComputerFactory();
        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, sorter, 0);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, hashPartitionComputerFactory, sortFields,
                comparatorFactories, nkmFactory), sorter, 0, writer, 0);
        spec.setFrameSize(frameSize);
        return spec;
    }

    @SuppressWarnings({ "rawtypes" })
    public JobSpecification scanIndexPrintGraph(String nodeName, String path) throws HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        Class<? extends Writable> vertexClass = BspUtils.getVertexClass(conf);
        JobSpecification spec = new JobSpecification();

        /**
         * construct empty tuple operator
         */
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        DataOutput dos = tb.getDataOutput();
        tb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize("0", dos);
        tb.addFieldEndOffset();
        ISerializerDeserializer[] keyRecDescSers = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);
        ConstantTupleSourceOperatorDescriptor emptyTupleSource = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        ClusterConfig.setLocationConstraint(spec, emptyTupleSource);

        /**
         * construct btree search operator
         */
        IConfigurationFactory confFactory = new ConfigurationFactory(conf);
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), vertexClass.getName());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);
        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);
        BTreeSearchOperatorDescriptor scanner = new BTreeSearchOperatorDescriptor(spec, recordDescriptor,
                storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories, null,
                null, null, true, true, getIndexDataflowHelperFactory(), false, NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, scanner);

        /**
         * construct write file operator
         */
        FileSplit resultFile = new FileSplit(nodeName, new FileReference(new File(path)));
        FileSplit[] results = new FileSplit[1];
        results[0] = resultFile;
        IFileSplitProvider resultFileSplitProvider = new ConstantFileSplitProvider(results);
        IRuntimeHookFactory preHookFactory = new RuntimeHookFactory(confFactory);
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                vertexIdClass.getName(), vertexClass.getName());
        VertexWriteOperatorDescriptor writer = new VertexWriteOperatorDescriptor(spec, inputRdFactory,
                resultFileSplitProvider, preHookFactory, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, writer, new String[] { "nc1" });
        PartitionConstraintHelper.addPartitionCountConstraint(spec, writer, 1);

        /**
         * connect operator descriptors
         */
        int[] sortFields = new int[1];
        sortFields[0] = 0;
        INormalizedKeyComputerFactory nkmFactory = JobGenUtil.getINormalizedKeyComputerFactory(conf);
        ITuplePartitionComputerFactory hashPartitionComputerFactory = getVertexPartitionComputerFactory();
        spec.connect(new OneToOneConnectorDescriptor(spec), emptyTupleSource, 0, scanner, 0);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, hashPartitionComputerFactory, sortFields,
                comparatorFactories, nkmFactory), scanner, 0, writer, 0);
        spec.setFrameSize(frameSize);
        return spec;
    }

    public JobSpecification scanIndexWriteGraph() throws HyracksException {
        JobSpecification spec = scanIndexWriteToHDFS(conf, false);
        return spec;
    }

    @Override
    public JobSpecification[] generateCheckpointing(int lastSuccessfulIteration) throws HyracksException {
        try {
            PregelixJob tmpJob = this.createCloneJob("Vertex checkpointing for job " + jobId, pregelixJob);
            tmpJob.setVertexOutputFormatClass(InternalVertexOutputFormat.class);
            FileOutputFormat.setOutputPath(tmpJob,
                    new Path(BspUtils.getVertexCheckpointPath(conf, lastSuccessfulIteration)));
            tmpJob.setOutputKeyClass(NullWritable.class);
            tmpJob.setOutputValueClass(BspUtils.getVertexClass(tmpJob.getConfiguration()));
            FileSystem dfs = FileSystem.get(tmpJob.getConfiguration());

            dfs.delete(new Path(BspUtils.getVertexCheckpointPath(conf, lastSuccessfulIteration)), true);
            JobSpecification vertexCkpSpec = scanIndexWriteToHDFS(tmpJob.getConfiguration(), true);

            dfs.delete(new Path(BspUtils.getMessageCheckpointPath(conf, lastSuccessfulIteration)), true);
            JobSpecification[] stateCkpSpecs = generateStateCheckpointing(lastSuccessfulIteration);
            JobSpecification[] specs = new JobSpecification[1 + stateCkpSpecs.length];

            specs[0] = vertexCkpSpec;
            for (int i = 1; i < specs.length; i++) {
                specs[i] = stateCkpSpecs[i - 1];
            }
            return specs;
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    @Override
    public JobSpecification generateLoadingJob() throws HyracksException {
        JobSpecification spec = loadHDFSData(pregelixJob);
        return spec;
    }

    public void clearCheckpoints() throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        // clear the checkpoint directory
        dfs.delete(new Path("/tmp/ckpoint/" + jobId), true);
    }

    @Override
    public JobSpecification[] generateLoadingCheckpoint(int lastCheckpointedIteration) throws HyracksException {
        try {
            PregelixJob tmpJob = this.createCloneJob("Vertex checkpoint loading for job " + jobId, pregelixJob);
            tmpJob.setVertexInputFormatClass(InternalVertexInputFormat.class);
            FileInputFormat.setInputPaths(tmpJob,
                    new Path(BspUtils.getVertexCheckpointPath(conf, lastCheckpointedIteration)));
            JobSpecification[] cleanVertices = generateCleanup();
            JobSpecification createIndex = generateCreatingJob();
            JobSpecification vertexLoadSpec = loadHDFSData(tmpJob);
            JobSpecification[] stateLoadSpecs = generateStateCheckpointLoading(lastCheckpointedIteration, tmpJob);
            JobSpecification[] specs = new JobSpecification[cleanVertices.length + 2 + stateLoadSpecs.length];

            int i = 0;
            for (; i < cleanVertices.length; i++) {
                specs[i] = cleanVertices[i];
            }
            specs[i++] = createIndex;
            specs[i++] = vertexLoadSpec;
            for (; i < specs.length; i++) {
                specs[i] = stateLoadSpecs[i - cleanVertices.length - 2];
            }
            return specs;
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    /***
     * generate a "clear state" job
     */
    public JobSpecification generateClearState() throws HyracksException {
        JobSpecification spec = new JobSpecification();
        ClearStateOperatorDescriptor clearState = new ClearStateOperatorDescriptor(spec, jobId);
        ClusterConfig.setLocationConstraint(spec, clearState);
        spec.addRoot(clearState);
        return spec;
    }

    /***
     * drop the sindex
     * 
     * @return JobSpecification
     * @throws HyracksException
     */
    protected JobSpecification dropIndex(String indexName) throws HyracksException {
        JobSpecification spec = new JobSpecification();

        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, indexName);
        IndexDropOperatorDescriptor drop = new IndexDropOperatorDescriptor(spec, storageManagerInterface,
                lcManagerProvider, fileSplitProvider, getIndexDataflowHelperFactory());

        ClusterConfig.setLocationConstraint(spec, drop);
        spec.addRoot(drop);
        spec.setFrameSize(frameSize);
        return spec;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected ITuplePartitionComputerFactory getVertexPartitionComputerFactory() {
        IConfigurationFactory confFactory = new ConfigurationFactory(conf);
        Class<? extends VertexPartitioner> partitionerClazz = BspUtils.getVertexPartitionerClass(conf);
        if (partitionerClazz != null) {
            return new VertexPartitionComputerFactory(confFactory);
        } else {
            return new VertexIdPartitionComputerFactory(new WritableSerializerDeserializerFactory(
                    BspUtils.getVertexIndexClass(conf)));
        }
    }

    protected IIndexDataflowHelperFactory getIndexDataflowHelperFactory() {
        if (BspUtils.useLSM(conf)) {
            return new LSMBTreeDataflowHelperFactory(new VirtualBufferCacheProvider(),
                    new ConstantMergePolicyFactory(), MERGE_POLICY_PROPERTIES, NoOpOperationTrackerProvider.INSTANCE,
                    SynchronousSchedulerProvider.INSTANCE, NoOpIOOperationCallback.INSTANCE, 0.01);
        } else {
            return new BTreeDataflowHelperFactory();
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private JobSpecification loadHDFSData(PregelixJob job) throws HyracksException, HyracksDataException {
        Configuration conf = job.getConfiguration();
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        Class<? extends Writable> vertexClass = BspUtils.getVertexClass(conf);
        JobSpecification spec = new JobSpecification();
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);

        /**
         * the graph file scan operator and use count constraint first, will use
         * absolute constraint later
         */
        VertexInputFormat inputFormat = BspUtils.createVertexInputFormat(conf);
        List<InputSplit> splits = new ArrayList<InputSplit>();
        try {
            splits = inputFormat.getSplits(job, fileSplitProvider.getFileSplits().length);
            LOGGER.info("number of splits: " + splits.size());
            for (InputSplit split : splits)
                LOGGER.info(split.toString());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), vertexClass.getName());
        IConfigurationFactory confFactory = new ConfigurationFactory(conf);
        String[] readSchedule = ClusterConfig.getHdfsScheduler().getLocationConstraints(splits);
        VertexFileScanOperatorDescriptor scanner = new VertexFileScanOperatorDescriptor(spec, recordDescriptor, splits,
                readSchedule, confFactory);
        ClusterConfig.setLocationConstraint(spec, scanner);

        /**
         * construct sort operator
         */
        int[] sortFields = new int[1];
        sortFields[0] = 0;
        INormalizedKeyComputerFactory nkmFactory = RawNormalizedKeyComputerFactory.INSTANCE;
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;
        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(spec, maxFrameNumber, sortFields,
                nkmFactory, comparatorFactories, recordDescriptor);
        ClusterConfig.setLocationConstraint(spec, sorter);

        /**
         * construct tree bulk load operator
         */
        int[] fieldPermutation = new int[2];
        fieldPermutation[0] = 0;
        fieldPermutation[1] = 1;
        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);
        TreeIndexBulkLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkLoadOperatorDescriptor(spec,
                storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories,
                sortFields, fieldPermutation, DEFAULT_BTREE_FILL_FACTOR, true, 0, false,
                getIndexDataflowHelperFactory(), NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, btreeBulkLoad);

        /**
         * connect operator descriptors
         */
        ITuplePartitionComputerFactory hashPartitionComputerFactory = getVertexPartitionComputerFactory();
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, hashPartitionComputerFactory), scanner, 0, sorter, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sorter, 0, btreeBulkLoad, 0);
        spec.setFrameSize(frameSize);
        return spec;
    }

    @SuppressWarnings({ "rawtypes" })
    private JobSpecification scanIndexWriteToHDFS(Configuration conf, boolean ckpointing) throws HyracksDataException,
            HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        Class<? extends Writable> vertexClass = BspUtils.getVertexClass(conf);
        JobSpecification spec = new JobSpecification();

        /**
         * construct empty tuple operator
         */
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        DataOutput dos = tb.getDataOutput();
        tb.reset();
        UTF8StringSerializerDeserializer.INSTANCE.serialize("0", dos);
        tb.addFieldEndOffset();
        ISerializerDeserializer[] keyRecDescSers = { UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        RecordDescriptor keyRecDesc = new RecordDescriptor(keyRecDescSers);
        ConstantTupleSourceOperatorDescriptor emptyTupleSource = new ConstantTupleSourceOperatorDescriptor(spec,
                keyRecDesc, tb.getFieldEndOffsets(), tb.getByteArray(), tb.getSize());
        ClusterConfig.setLocationConstraint(spec, emptyTupleSource);

        /**
         * construct btree search operator
         */
        IConfigurationFactory confFactory = new ConfigurationFactory(conf);
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), vertexClass.getName());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);

        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);

        BTreeSearchOperatorDescriptor scanner = new BTreeSearchOperatorDescriptor(spec, recordDescriptor,
                storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories, null,
                null, null, true, true, getIndexDataflowHelperFactory(), false, NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, scanner);

        ExternalSortOperatorDescriptor sort = null;
        if (!ckpointing) {
            int[] keyFields = new int[] { 0 };
            INormalizedKeyComputerFactory nkmFactory = JobGenUtil.getFinalNormalizedKeyComputerFactory(conf);
            IBinaryComparatorFactory[] sortCmpFactories = new IBinaryComparatorFactory[1];
            sortCmpFactories[0] = JobGenUtil.getFinalBinaryComparatorFactory(WritableComparator.get(vertexIdClass)
                    .getClass());
            sort = new ExternalSortOperatorDescriptor(spec, maxFrameNumber, keyFields, nkmFactory, sortCmpFactories,
                    recordDescriptor);
            ClusterConfig.setLocationConstraint(spec, scanner);
        }

        /**
         * construct write file operator
         */
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                vertexIdClass.getName(), vertexClass.getName());
        VertexFileWriteOperatorDescriptor writer = new VertexFileWriteOperatorDescriptor(spec, confFactory,
                inputRdFactory);
        ClusterConfig.setLocationConstraint(spec, writer);

        /**
         * connect operator descriptors
         */
        spec.connect(new OneToOneConnectorDescriptor(spec), emptyTupleSource, 0, scanner, 0);
        if (!ckpointing) {
            spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, sort, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), sort, 0, writer, 0);
        } else {
            spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, writer, 0);
        }
        spec.setFrameSize(frameSize);
        return spec;
    }

    protected PregelixJob createCloneJob(String newJobName, PregelixJob oldJob) throws HyracksException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            oldJob.getConfiguration().write(dos);
            PregelixJob newJob = new PregelixJob(newJobName);
            DataInput dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
            newJob.getConfiguration().readFields(dis);
            return newJob;
        } catch (IOException e) {
            throw new HyracksException(e);
        }
    }

    /** generate plan specific state checkpointing */
    protected JobSpecification[] generateStateCheckpointing(int lastSuccessfulIteration) throws HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        JobSpecification spec = new JobSpecification();

        /**
         * source aggregate
         */
        RecordDescriptor rdFinal = DataflowUtils.getRecordDescriptorFromKeyValueClasses(vertexIdClass.getName(),
                MsgList.class.getName());

        /**
         * construct empty tuple operator
         */
        EmptyTupleSourceOperatorDescriptor emptyTupleSource = new EmptyTupleSourceOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptyTupleSource);

        /**
         * construct the materializing write operator
         */
        MaterializingReadOperatorDescriptor materializeRead = new MaterializingReadOperatorDescriptor(spec, rdFinal,
                false);
        ClusterConfig.setLocationConstraint(spec, materializeRead);

        String checkpointPath = BspUtils.getMessageCheckpointPath(conf, lastSuccessfulIteration);;
        PregelixJob tmpJob = createCloneJob("State checkpointing for job " + jobId, pregelixJob);
        tmpJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(tmpJob, new Path(checkpointPath));
        tmpJob.setOutputKeyClass(vertexIdClass);
        tmpJob.setOutputValueClass(MsgList.class);

        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                vertexIdClass.getName(), MsgList.class.getName());
        HDFSFileWriteOperatorDescriptor hdfsWriter = new HDFSFileWriteOperatorDescriptor(spec, tmpJob, inputRdFactory);
        ClusterConfig.setLocationConstraint(spec, hdfsWriter);

        spec.connect(new OneToOneConnectorDescriptor(spec), emptyTupleSource, 0, materializeRead, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materializeRead, 0, hdfsWriter, 0);
        spec.setFrameSize(frameSize);
        return new JobSpecification[] { spec };
    }

    /** load plan specific state checkpoints */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected JobSpecification[] generateStateCheckpointLoading(int lastCheckpointedIteration, PregelixJob job)
            throws HyracksException {
        String checkpointPath = BspUtils.getMessageCheckpointPath(job.getConfiguration(), lastCheckpointedIteration);
        PregelixJob tmpJob = createCloneJob("State checkpoint loading for job " + jobId, job);
        tmpJob.setInputFormatClass(SequenceFileInputFormat.class);
        try {
            FileInputFormat.setInputPaths(tmpJob, checkpointPath);
        } catch (IOException e) {
            throw new HyracksException(e);
        }
        Configuration conf = tmpJob.getConfiguration();
        Class vertexIdClass = BspUtils.getVertexIndexClass(conf);
        JobSpecification spec = new JobSpecification();

        /***
         * HDFS read operator
         */
        List<InputSplit> splits = new ArrayList<InputSplit>();
        try {
            InputFormat inputFormat = org.apache.hadoop.util.ReflectionUtils.newInstance(job.getInputFormatClass(),
                    job.getConfiguration());
            splits = inputFormat.getSplits(tmpJob);
            LOGGER.info("number of splits: " + splits.size());
            for (InputSplit split : splits)
                LOGGER.info(split.toString());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), MsgList.class.getName());
        String[] readSchedule = ClusterConfig.getHdfsScheduler().getLocationConstraints(splits);
        HDFSReadOperatorDescriptor scanner = new HDFSReadOperatorDescriptor(spec, recordDescriptor, tmpJob, splits,
                readSchedule, new KeyValueParserFactory());
        ClusterConfig.setLocationConstraint(spec, scanner);

        /** construct the sort operator to sort message states */
        int[] keyFields = new int[] { 0 };
        INormalizedKeyComputerFactory nkmFactory = JobGenUtil.getINormalizedKeyComputerFactory(conf);
        IBinaryComparatorFactory[] sortCmpFactories = new IBinaryComparatorFactory[1];
        sortCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(lastCheckpointedIteration,
                WritableComparator.get(vertexIdClass).getClass());
        ExternalSortOperatorDescriptor sort = new ExternalSortOperatorDescriptor(spec, maxFrameNumber, keyFields,
                nkmFactory, sortCmpFactories, recordDescriptor, Algorithm.QUICK_SORT);
        ClusterConfig.setLocationConstraint(spec, sort);

        /**
         * construct the materializing write operator
         */
        MaterializingWriteOperatorDescriptor materialize = new MaterializingWriteOperatorDescriptor(spec,
                recordDescriptor);
        ClusterConfig.setLocationConstraint(spec, materialize);

        /** construct runtime hook */
        RuntimeHookOperatorDescriptor postSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new RecoveryRuntimeHookFactory(jobId, lastCheckpointedIteration, new ConfigurationFactory(
                        pregelixJob.getConfiguration())));
        ClusterConfig.setLocationConstraint(spec, postSuperStep);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink = new EmptySinkOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptySink);

        /**
         * connect operator descriptors
         */
        ITuplePartitionComputerFactory hashPartitionComputerFactory = getVertexPartitionComputerFactory();
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, hashPartitionComputerFactory), scanner, 0, sort, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sort, 0, materialize, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materialize, 0, postSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), postSuperStep, 0, emptySink, 0);
        spec.setFrameSize(frameSize);
        return new JobSpecification[] { spec };
    }

    /** generate non-first iteration job */
    protected abstract JobSpecification generateNonFirstIteration(int iteration) throws HyracksException;

    /** generate first iteration job */
    protected abstract JobSpecification generateFirstIteration(int iteration) throws HyracksException;

    /** generate clean-up job */
    public abstract JobSpecification[] generateCleanup() throws HyracksException;

}
