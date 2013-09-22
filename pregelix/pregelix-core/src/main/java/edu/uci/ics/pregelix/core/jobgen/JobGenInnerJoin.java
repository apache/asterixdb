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

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;
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
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.ConstantTupleSourceOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.Algorithm;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs2.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexInsertUpdateDeleteOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.pregelix.api.graph.MsgList;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.core.data.TypeTraits;
import edu.uci.ics.pregelix.core.hadoop.config.ConfigurationFactory;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.runtime.touchpoint.RawBinaryComparatorFactory;
import edu.uci.ics.pregelix.core.util.DataflowUtils;
import edu.uci.ics.pregelix.dataflow.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.pregelix.dataflow.EmptySinkOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.EmptyTupleSourceOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.FinalAggregateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.HDFSFileWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.KeyValueParserFactory;
import edu.uci.ics.pregelix.dataflow.MaterializingReadOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.MaterializingWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.TerminationStateWriterOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.group.ClusteredGroupOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.group.IClusteredAggregatorDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.IndexNestedLoopJoinFunctionUpdateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.IndexNestedLoopJoinOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.RuntimeHookOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.TreeIndexBulkReLoadOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.TreeSearchFunctionUpdateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.function.ComputeUpdateFunctionFactory;
import edu.uci.ics.pregelix.runtime.function.StartComputeUpdateFunctionFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.MergePartitionComputerFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.PostSuperStepRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.PreSuperStepRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.RuntimeHookFactory;

public class JobGenInnerJoin extends JobGen {
    private static final Logger LOGGER = Logger.getLogger(JobGen.class.getName());

    public JobGenInnerJoin(PregelixJob job) {
        super(job);
    }

    protected JobSpecification generateFirstIteration(int iteration) throws HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        Class<? extends Writable> vertexClass = BspUtils.getVertexClass(conf);
        Class<? extends Writable> messageValueClass = BspUtils.getMessageValueClass(conf);
        Class<? extends Writable> partialAggregateValueClass = BspUtils.getPartialAggregateValueClass(conf);
        IConfigurationFactory confFactory = new ConfigurationFactory(conf);
        JobSpecification spec = new JobSpecification();

        /**
         * construct empty tuple operator
         */
        EmptyTupleSourceOperatorDescriptor emptyTupleSource = new EmptyTupleSourceOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptyTupleSource);

        /** construct runtime hook */
        RuntimeHookOperatorDescriptor preSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PreSuperStepRuntimeHookFactory(jobId, confFactory));
        ClusterConfig.setLocationConstraint(spec, preSuperStep);

        /**
         * construct drop index operator
         */
        IFileSplitProvider secondaryFileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, SECONDARY_INDEX_ODD);
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);

        /**
         * construct btree search and function call update operator
         */
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), vertexClass.getName());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;

        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);

        RecordDescriptor rdDummy = DataflowUtils.getRecordDescriptorFromWritableClasses(VLongWritable.class.getName());
        RecordDescriptor rdPartialAggregate = DataflowUtils
                .getRecordDescriptorFromWritableClasses(partialAggregateValueClass.getName());
        IConfigurationFactory configurationFactory = new ConfigurationFactory(conf);
        IRuntimeHookFactory preHookFactory = new RuntimeHookFactory(configurationFactory);
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                vertexIdClass.getName(), vertexClass.getName());
        RecordDescriptor rdFinal = DataflowUtils.getRecordDescriptorFromKeyValueClasses(vertexIdClass.getName(),
                MsgList.class.getName());
        RecordDescriptor rdUnnestedMessage = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), messageValueClass.getName());
        RecordDescriptor rdInsert = DataflowUtils.getRecordDescriptorFromKeyValueClasses(vertexIdClass.getName(),
                vertexClass.getName());
        RecordDescriptor rdDelete = DataflowUtils.getRecordDescriptorFromWritableClasses(vertexIdClass.getName());

        TreeSearchFunctionUpdateOperatorDescriptor scanner = new TreeSearchFunctionUpdateOperatorDescriptor(spec,
                recordDescriptor, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, JobGenUtil.getForwardScan(iteration), null, null, true, true,
                getIndexDataflowHelperFactory(), inputRdFactory, 6, new StartComputeUpdateFunctionFactory(confFactory),
                preHookFactory, null, rdUnnestedMessage, rdDummy, rdPartialAggregate, rdInsert, rdDelete, rdFinal);
        ClusterConfig.setLocationConstraint(spec, scanner);

        /**
         * termination state write operator
         */
        TerminationStateWriterOperatorDescriptor terminateWriter = new TerminationStateWriterOperatorDescriptor(spec,
                configurationFactory, jobId);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, terminateWriter, 1);

        /**
         * final aggregate write operator
         */
        IRecordDescriptorFactory aggRdFactory = DataflowUtils
                .getWritableRecordDescriptorFactoryFromWritableClasses(partialAggregateValueClass.getName());
        FinalAggregateOperatorDescriptor finalAggregator = new FinalAggregateOperatorDescriptor(spec,
                configurationFactory, aggRdFactory, jobId);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, finalAggregator, 1);

        /**
         * construct bulk-load index operator
         */
        int[] fieldPermutation = new int[] { 0, 1 };
        int[] keyFields = new int[] { 0 };
        IBinaryComparatorFactory[] indexCmpFactories = new IBinaryComparatorFactory[1];
        indexCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(iteration + 1,
                WritableComparator.get(vertexIdClass).getClass());
        TreeIndexBulkReLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkReLoadOperatorDescriptor(spec,
                storageManagerInterface, lcManagerProvider, secondaryFileSplitProvider, typeTraits, indexCmpFactories,
                fieldPermutation, keyFields, DEFAULT_BTREE_FILL_FACTOR, getIndexDataflowHelperFactory());
        ClusterConfig.setLocationConstraint(spec, btreeBulkLoad);

        /**
         * construct local sort operator
         */
        INormalizedKeyComputerFactory nkmFactory = JobGenUtil.getINormalizedKeyComputerFactory(conf);
        IBinaryComparatorFactory[] sortCmpFactories = new IBinaryComparatorFactory[1];
        sortCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(iteration, WritableComparator.get(vertexIdClass)
                .getClass());
        ExternalSortOperatorDescriptor localSort = new ExternalSortOperatorDescriptor(spec, maxFrameNumber, keyFields,
                nkmFactory, sortCmpFactories, rdUnnestedMessage, Algorithm.QUICK_SORT);
        ClusterConfig.setLocationConstraint(spec, localSort);

        /**
         * construct local pre-clustered group-by operator
         */
        IClusteredAggregatorDescriptorFactory aggregatorFactory = DataflowUtils.getAccumulatingAggregatorFactory(conf,
                false, false);
        ClusteredGroupOperatorDescriptor localGby = new ClusteredGroupOperatorDescriptor(spec, keyFields,
                sortCmpFactories, aggregatorFactory, rdUnnestedMessage);
        ClusterConfig.setLocationConstraint(spec, localGby);

        /**
         * construct global group-by operator
         */
        IClusteredAggregatorDescriptorFactory aggregatorFactoryFinal = DataflowUtils.getAccumulatingAggregatorFactory(
                conf, true, true);
        ClusteredGroupOperatorDescriptor globalGby = new ClusteredGroupOperatorDescriptor(spec, keyFields,
                sortCmpFactories, aggregatorFactoryFinal, rdFinal);
        ClusterConfig.setLocationConstraint(spec, globalGby);

        /**
         * construct the materializing write operator
         */
        MaterializingWriteOperatorDescriptor materialize = new MaterializingWriteOperatorDescriptor(spec, rdFinal);
        ClusterConfig.setLocationConstraint(spec, materialize);

        /**
         * do pre- & post- super step
         */
        RuntimeHookOperatorDescriptor postSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PostSuperStepRuntimeHookFactory(jobId));
        ClusterConfig.setLocationConstraint(spec, postSuperStep);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink = new EmptySinkOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptySink);

        /**
         * add the insert operator to insert vertexes
         */
        TreeIndexInsertUpdateDeleteOperatorDescriptor insertOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, rdInsert, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, fieldPermutation, IndexOperation.INSERT, getIndexDataflowHelperFactory(),
                null, NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, insertOp);

        /**
         * add the delete operator to delete vertexes
         */
        int[] fieldPermutationDelete = new int[] { 0 };
        TreeIndexInsertUpdateDeleteOperatorDescriptor deleteOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, rdDelete, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, fieldPermutationDelete, IndexOperation.DELETE,
                getIndexDataflowHelperFactory(), null, NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, deleteOp);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink3 = new EmptySinkOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptySink3);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink4 = new EmptySinkOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptySink4);

        ITuplePartitionComputerFactory partionFactory = getVertexPartitionComputerFactory();
        ITuplePartitionComputerFactory unifyingPartitionComputerFactory = new MergePartitionComputerFactory();

        /** connect all operators **/
        spec.connect(new OneToOneConnectorDescriptor(spec), emptyTupleSource, 0, preSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), preSuperStep, 0, scanner, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, localSort, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, unifyingPartitionComputerFactory), scanner, 1,
                terminateWriter, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, unifyingPartitionComputerFactory), scanner, 2,
                finalAggregator, 0);

        /**
         * connect the insert/delete operator
         */
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), scanner, 3, insertOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), insertOp, 0, emptySink3, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), scanner, 4, deleteOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), deleteOp, 0, emptySink4, 0);

        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), scanner, 5, btreeBulkLoad, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), localSort, 0, localGby, 0);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, partionFactory, keyFields, sortCmpFactories,
                nkmFactory), localGby, 0, globalGby, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), globalGby, 0, materialize, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materialize, 0, postSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), postSuperStep, 0, emptySink, 0);

        spec.addRoot(emptySink);
        spec.addRoot(btreeBulkLoad);
        spec.addRoot(terminateWriter);
        spec.addRoot(finalAggregator);
        spec.addRoot(emptySink3);
        spec.addRoot(emptySink4);

        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy(spec));
        spec.setFrameSize(frameSize);
        return spec;
    }

    @Override
    protected JobSpecification generateNonFirstIteration(int iteration) throws HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(conf);
        Class<? extends Writable> vertexClass = BspUtils.getVertexClass(conf);
        Class<? extends Writable> messageValueClass = BspUtils.getMessageValueClass(conf);
        Class<? extends Writable> partialAggregateValueClass = BspUtils.getPartialAggregateValueClass(conf);
        JobSpecification spec = new JobSpecification();

        /**
         * source aggregate
         */
        int[] keyFields = new int[] { 0 };
        RecordDescriptor rdUnnestedMessage = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), messageValueClass.getName());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;
        RecordDescriptor rdFinal = DataflowUtils.getRecordDescriptorFromKeyValueClasses(vertexIdClass.getName(),
                MsgList.class.getName());
        RecordDescriptor rdInsert = DataflowUtils.getRecordDescriptorFromKeyValueClasses(vertexIdClass.getName(),
                vertexClass.getName());
        RecordDescriptor rdDelete = DataflowUtils.getRecordDescriptorFromWritableClasses(vertexIdClass.getName());

        /**
         * construct empty tuple operator
         */
        EmptyTupleSourceOperatorDescriptor emptyTupleSource = new EmptyTupleSourceOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptyTupleSource);

        /**
         * construct pre-superstep
         */
        IConfigurationFactory confFactory = new ConfigurationFactory(conf);
        RuntimeHookOperatorDescriptor preSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PreSuperStepRuntimeHookFactory(jobId, confFactory));
        ClusterConfig.setLocationConstraint(spec, preSuperStep);

        /**
         * construct the materializing write operator
         */
        MaterializingReadOperatorDescriptor materializeRead = new MaterializingReadOperatorDescriptor(spec, rdFinal,
                true);
        ClusterConfig.setLocationConstraint(spec, materializeRead);

        /**
         * construct the index-set-union operator
         */
        String readFile = iteration % 2 == 0 ? SECONDARY_INDEX_ODD : SECONDARY_INDEX_EVEN;
        IFileSplitProvider secondaryFileSplitProviderRead = ClusterConfig.getFileSplitProvider(jobId, readFile);

        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);
        IndexNestedLoopJoinOperatorDescriptor setUnion = new IndexNestedLoopJoinOperatorDescriptor(spec, rdFinal,
                storageManagerInterface, lcManagerProvider, secondaryFileSplitProviderRead, typeTraits,
                comparatorFactories, true, keyFields, keyFields, true, true, getIndexDataflowHelperFactory(), true);
        ClusterConfig.setLocationConstraint(spec, setUnion);

        /**
         * construct index-join-function-update operator
         */
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);
        RecordDescriptor rdDummy = DataflowUtils.getRecordDescriptorFromWritableClasses(VLongWritable.class.getName());
        RecordDescriptor rdPartialAggregate = DataflowUtils
                .getRecordDescriptorFromWritableClasses(partialAggregateValueClass.getName());
        IConfigurationFactory configurationFactory = new ConfigurationFactory(conf);
        IRuntimeHookFactory preHookFactory = new RuntimeHookFactory(configurationFactory);
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                vertexIdClass.getName(), MsgList.class.getName(), vertexIdClass.getName(), vertexClass.getName());

        IndexNestedLoopJoinFunctionUpdateOperatorDescriptor join = new IndexNestedLoopJoinFunctionUpdateOperatorDescriptor(
                spec, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits, comparatorFactories,
                JobGenUtil.getForwardScan(iteration), keyFields, keyFields, true, true,
                getIndexDataflowHelperFactory(), inputRdFactory, 6, new ComputeUpdateFunctionFactory(confFactory),
                preHookFactory, null, rdUnnestedMessage, rdDummy, rdPartialAggregate, rdInsert, rdDelete, rdFinal);
        ClusterConfig.setLocationConstraint(spec, join);

        /**
         * construct bulk-load index operator
         */
        int fieldPermutation[] = new int[] { 0, 1 };
        IBinaryComparatorFactory[] indexCmpFactories = new IBinaryComparatorFactory[1];
        indexCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(iteration + 1,
                WritableComparator.get(vertexIdClass).getClass());
        String writeFile = iteration % 2 == 0 ? SECONDARY_INDEX_EVEN : SECONDARY_INDEX_ODD;
        IFileSplitProvider secondaryFileSplitProviderWrite = ClusterConfig.getFileSplitProvider(jobId, writeFile);
        TreeIndexBulkReLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkReLoadOperatorDescriptor(spec,
                storageManagerInterface, lcManagerProvider, secondaryFileSplitProviderWrite, typeTraits,
                indexCmpFactories, fieldPermutation, keyFields, DEFAULT_BTREE_FILL_FACTOR,
                getIndexDataflowHelperFactory());
        ClusterConfig.setLocationConstraint(spec, btreeBulkLoad);

        /**
         * construct local sort operator
         */
        INormalizedKeyComputerFactory nkmFactory = JobGenUtil.getINormalizedKeyComputerFactory(conf);
        IBinaryComparatorFactory[] sortCmpFactories = new IBinaryComparatorFactory[1];
        sortCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(iteration, WritableComparator.get(vertexIdClass)
                .getClass());
        ExternalSortOperatorDescriptor localSort = new ExternalSortOperatorDescriptor(spec, maxFrameNumber, keyFields,
                nkmFactory, sortCmpFactories, rdUnnestedMessage, Algorithm.QUICK_SORT);
        ClusterConfig.setLocationConstraint(spec, localSort);

        /**
         * construct local pre-clustered group-by operator
         */
        IClusteredAggregatorDescriptorFactory aggregatorFactory = DataflowUtils.getAccumulatingAggregatorFactory(conf,
                false, false);
        ClusteredGroupOperatorDescriptor localGby = new ClusteredGroupOperatorDescriptor(spec, keyFields,
                sortCmpFactories, aggregatorFactory, rdUnnestedMessage);
        ClusterConfig.setLocationConstraint(spec, localGby);

        /**
         * construct global group-by operator
         */
        IClusteredAggregatorDescriptorFactory aggregatorFactoryFinal = DataflowUtils.getAccumulatingAggregatorFactory(
                conf, true, true);
        ClusteredGroupOperatorDescriptor globalGby = new ClusteredGroupOperatorDescriptor(spec, keyFields,
                sortCmpFactories, aggregatorFactoryFinal, rdFinal);
        ClusterConfig.setLocationConstraint(spec, globalGby);

        /**
         * construct the materializing write operator
         */
        MaterializingWriteOperatorDescriptor materialize = new MaterializingWriteOperatorDescriptor(spec, rdFinal);
        ClusterConfig.setLocationConstraint(spec, materialize);

        /** construct runtime hook */
        RuntimeHookOperatorDescriptor postSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PostSuperStepRuntimeHookFactory(jobId));
        ClusterConfig.setLocationConstraint(spec, postSuperStep);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink = new EmptySinkOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptySink);

        /**
         * termination state write operator
         */
        TerminationStateWriterOperatorDescriptor terminateWriter = new TerminationStateWriterOperatorDescriptor(spec,
                configurationFactory, jobId);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, terminateWriter, 1);

        /**
         * final aggregate write operator
         */
        IRecordDescriptorFactory aggRdFactory = DataflowUtils
                .getWritableRecordDescriptorFactoryFromWritableClasses(partialAggregateValueClass.getName());
        FinalAggregateOperatorDescriptor finalAggregator = new FinalAggregateOperatorDescriptor(spec,
                configurationFactory, aggRdFactory, jobId);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, finalAggregator, 1);

        /**
         * add the insert operator to insert vertexes
         */
        TreeIndexInsertUpdateDeleteOperatorDescriptor insertOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, rdInsert, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, fieldPermutation, IndexOperation.INSERT, getIndexDataflowHelperFactory(),
                null, NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, insertOp);

        /**
         * add the delete operator to delete vertexes
         */
        int[] fieldPermutationDelete = new int[] { 0 };
        TreeIndexInsertUpdateDeleteOperatorDescriptor deleteOp = new TreeIndexInsertUpdateDeleteOperatorDescriptor(
                spec, rdDelete, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, null, fieldPermutationDelete, IndexOperation.DELETE,
                getIndexDataflowHelperFactory(), null, NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, deleteOp);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink3 = new EmptySinkOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptySink3);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink4 = new EmptySinkOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptySink4);

        ITuplePartitionComputerFactory unifyingPartitionComputerFactory = new MergePartitionComputerFactory();
        ITuplePartitionComputerFactory partionFactory = getVertexPartitionComputerFactory();
        /** connect all operators **/
        spec.connect(new OneToOneConnectorDescriptor(spec), emptyTupleSource, 0, preSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), preSuperStep, 0, materializeRead, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materializeRead, 0, setUnion, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), setUnion, 0, join, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), join, 0, localSort, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, unifyingPartitionComputerFactory), join, 1,
                terminateWriter, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, unifyingPartitionComputerFactory), join, 2,
                finalAggregator, 0);

        /**
         * connect the insert/delete operator
         */
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), join, 3, insertOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), insertOp, 0, emptySink3, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), join, 4, deleteOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), deleteOp, 0, emptySink4, 0);

        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), join, 5, btreeBulkLoad, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), localSort, 0, localGby, 0);
        spec.connect(new MToNPartitioningMergingConnectorDescriptor(spec, partionFactory, keyFields, sortCmpFactories,
                nkmFactory), localGby, 0, globalGby, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), globalGby, 0, materialize, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materialize, 0, postSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), postSuperStep, 0, emptySink, 0);

        spec.addRoot(emptySink);
        spec.addRoot(btreeBulkLoad);
        spec.addRoot(terminateWriter);
        spec.addRoot(emptySink3);
        spec.addRoot(emptySink4);

        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy(spec));
        spec.setFrameSize(frameSize);
        return spec;
    }

    /** generate plan specific state checkpointing */
    protected JobSpecification[] generateStateCheckpointing(int lastSuccessfulIteration) throws HyracksException {
        JobSpecification[] msgCkpSpecs = super.generateStateCheckpointing(lastSuccessfulIteration);

        /** generate secondary index checkpoint */
        PregelixJob tmpJob = this.createCloneJob("Secondary index checkpointing for job " + jobId, pregelixJob);

        JobSpecification secondaryBTreeCkp = generateSecondaryBTreeCheckpoint(lastSuccessfulIteration, tmpJob);

        JobSpecification[] specs = new JobSpecification[msgCkpSpecs.length + 1];
        for (int i = 0; i < msgCkpSpecs.length; i++) {
            specs[i] = msgCkpSpecs[i];
        }
        specs[specs.length - 1] = secondaryBTreeCkp;
        return specs;
    }

    /**
     * generate plan specific checkpoint loading
     */
    @Override
    protected JobSpecification[] generateStateCheckpointLoading(int lastSuccessfulIteration, PregelixJob job)
            throws HyracksException {
        /** generate message checkpoint load */
        JobSpecification[] msgCkpSpecs = super.generateStateCheckpointLoading(lastSuccessfulIteration, job);

        /** generate secondary index checkpoint load */
        PregelixJob tmpJob = this.createCloneJob("Secondary index checkpoint loading for job " + jobId, pregelixJob);
        tmpJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        JobSpecification secondaryBTreeCkpLoad = generateSecondaryBTreeCheckpointLoad(lastSuccessfulIteration, tmpJob);
        JobSpecification[] specs = new JobSpecification[msgCkpSpecs.length + 1];
        for (int i = 0; i < msgCkpSpecs.length; i++) {
            specs[i] = msgCkpSpecs[i];
        }
        specs[specs.length - 1] = secondaryBTreeCkpLoad;
        return specs;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private JobSpecification generateSecondaryBTreeCheckpointLoad(int lastSuccessfulIteration, PregelixJob job)
            throws HyracksException {
        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(job.getConfiguration());
        JobSpecification spec = new JobSpecification();

        String checkpointPath = BspUtils.getSecondaryIndexCheckpointPath(conf, lastSuccessfulIteration);
        PregelixJob tmpJob = createCloneJob("State checkpoint loading for job " + jobId, job);
        tmpJob.setInputFormatClass(SequenceFileInputFormat.class);
        try {
            FileInputFormat.setInputPaths(tmpJob, checkpointPath);
        } catch (IOException e) {
            throw new HyracksException(e);
        }

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
        sortCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(lastSuccessfulIteration,
                WritableComparator.get(vertexIdClass).getClass());
        ExternalSortOperatorDescriptor sort = new ExternalSortOperatorDescriptor(spec, maxFrameNumber, keyFields,
                nkmFactory, sortCmpFactories, recordDescriptor, Algorithm.QUICK_SORT);
        ClusterConfig.setLocationConstraint(spec, sort);

        /**
         * construct bulk-load index operator
         */
        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);
        int fieldPermutation[] = new int[] { 0, 1 };
        IBinaryComparatorFactory[] indexCmpFactories = new IBinaryComparatorFactory[1];
        indexCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(lastSuccessfulIteration + 1, WritableComparator
                .get(vertexIdClass).getClass());
        String writeFile = lastSuccessfulIteration % 2 == 0 ? SECONDARY_INDEX_EVEN : SECONDARY_INDEX_ODD;
        IFileSplitProvider secondaryFileSplitProviderWrite = ClusterConfig.getFileSplitProvider(jobId, writeFile);
        TreeIndexBulkReLoadOperatorDescriptor btreeBulkLoad = new TreeIndexBulkReLoadOperatorDescriptor(spec,
                storageManagerInterface, lcManagerProvider, secondaryFileSplitProviderWrite, typeTraits,
                indexCmpFactories, fieldPermutation, new int[] { 0 }, DEFAULT_BTREE_FILL_FACTOR,
                getIndexDataflowHelperFactory());
        ClusterConfig.setLocationConstraint(spec, btreeBulkLoad);

        /**
         * connect operator descriptors
         */
        ITuplePartitionComputerFactory hashPartitionComputerFactory = getVertexPartitionComputerFactory();
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, hashPartitionComputerFactory), scanner, 0, sort, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sort, 0, btreeBulkLoad, 0);
        spec.setFrameSize(frameSize);

        return spec;
    }

    @SuppressWarnings({ "rawtypes" })
    private JobSpecification generateSecondaryBTreeCheckpoint(int lastSuccessfulIteration, PregelixJob job)
            throws HyracksException {
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        String checkpointPath = BspUtils.getSecondaryIndexCheckpointPath(conf, lastSuccessfulIteration);
        FileOutputFormat.setOutputPath(job, new Path(checkpointPath));
        job.setOutputKeyClass(BspUtils.getVertexIndexClass(job.getConfiguration()));
        job.setOutputValueClass(MsgList.class);

        Class<? extends WritableComparable<?>> vertexIdClass = BspUtils.getVertexIndexClass(job.getConfiguration());
        Class<? extends Writable> msgListClass = MsgList.class;
        String readFile = lastSuccessfulIteration % 2 == 0 ? SECONDARY_INDEX_EVEN : SECONDARY_INDEX_ODD;
        IFileSplitProvider secondaryFileSplitProviderRead = ClusterConfig.getFileSplitProvider(jobId, readFile);
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
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), msgListClass.getName());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;

        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);

        BTreeSearchOperatorDescriptor scanner = new BTreeSearchOperatorDescriptor(spec, recordDescriptor,
                storageManagerInterface, lcManagerProvider, secondaryFileSplitProviderRead, typeTraits,
                comparatorFactories, null, null, null, true, true, getIndexDataflowHelperFactory(), false,
                NoOpOperationCallbackFactory.INSTANCE);
        ClusterConfig.setLocationConstraint(spec, scanner);

        /**
         * construct write file operator
         */
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                vertexIdClass.getName(), MsgList.class.getName());
        HDFSFileWriteOperatorDescriptor writer = new HDFSFileWriteOperatorDescriptor(spec, job, inputRdFactory);
        ClusterConfig.setLocationConstraint(spec, writer);

        /**
         * connect operator descriptors
         */
        spec.connect(new OneToOneConnectorDescriptor(spec), emptyTupleSource, 0, scanner, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), scanner, 0, writer, 0);
        spec.setFrameSize(frameSize);
        return spec;
    }

    @Override
    public JobSpecification[] generateCleanup() throws HyracksException {
        JobSpecification[] cleanups = new JobSpecification[3];
        cleanups[0] = this.dropIndex(PRIMARY_INDEX);
        cleanups[1] = this.dropIndex(SECONDARY_INDEX_ODD);
        cleanups[2] = this.dropIndex(SECONDARY_INDEX_EVEN);
        return cleanups;
    }
}
