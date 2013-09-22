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

import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.sort.Algorithm;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
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
import edu.uci.ics.pregelix.dataflow.MaterializingReadOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.MaterializingWriteOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.TerminationStateWriterOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.group.ClusteredGroupOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.group.IClusteredAggregatorDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.IndexNestedLoopJoinFunctionUpdateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.RuntimeHookOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.TreeSearchFunctionUpdateOperatorDescriptor;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.function.ComputeUpdateFunctionFactory;
import edu.uci.ics.pregelix.runtime.function.StartComputeUpdateFunctionFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.MergePartitionComputerFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.MsgListNullWriterFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.PostSuperStepRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.PreSuperStepRuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.RuntimeHookFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.VertexIdNullWriterFactory;

public class JobGenOuterJoinSingleSort extends JobGen {

    public JobGenOuterJoinSingleSort(PregelixJob job) {
        super(job);
    }

    @Override
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
         * construct btree search operator
         */
        RecordDescriptor recordDescriptor = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), vertexClass.getName());
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[1];
        comparatorFactories[0] = RawBinaryComparatorFactory.INSTANCE;
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);

        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);

        /**
         * construct compute operator
         */
        RecordDescriptor rdDummy = DataflowUtils.getRecordDescriptorFromWritableClasses(VLongWritable.class.getName());
        RecordDescriptor rdPartialAggregate = DataflowUtils
                .getRecordDescriptorFromWritableClasses(partialAggregateValueClass.getName());
        IConfigurationFactory configurationFactory = new ConfigurationFactory(conf);
        IRuntimeHookFactory preHookFactory = new RuntimeHookFactory(configurationFactory);
        IRecordDescriptorFactory inputRdFactory = DataflowUtils.getWritableRecordDescriptorFactoryFromWritableClasses(
                vertexIdClass.getName(), vertexClass.getName());
        RecordDescriptor rdUnnestedMessage = DataflowUtils.getRecordDescriptorFromKeyValueClasses(
                vertexIdClass.getName(), messageValueClass.getName());
        RecordDescriptor rdInsert = DataflowUtils.getRecordDescriptorFromKeyValueClasses(vertexIdClass.getName(),
                vertexClass.getName());
        RecordDescriptor rdDelete = DataflowUtils.getRecordDescriptorFromWritableClasses(vertexIdClass.getName());

        TreeSearchFunctionUpdateOperatorDescriptor scanner = new TreeSearchFunctionUpdateOperatorDescriptor(spec,
                recordDescriptor, storageManagerInterface, lcManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, JobGenUtil.getForwardScan(iteration), null, null, true, true,
                getIndexDataflowHelperFactory(), inputRdFactory, 5, new StartComputeUpdateFunctionFactory(confFactory),
                preHookFactory, null, rdUnnestedMessage, rdDummy, rdPartialAggregate, rdInsert, rdDelete);
        ClusterConfig.setLocationConstraint(spec, scanner);

        /**
         * construct global sort operator
         */
        int[] keyFields = new int[] { 0 };
        INormalizedKeyComputerFactory nkmFactory = JobGenUtil.getINormalizedKeyComputerFactory(conf);
        IBinaryComparatorFactory[] sortCmpFactories = new IBinaryComparatorFactory[1];
        sortCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(iteration, WritableComparator.get(vertexIdClass)
                .getClass());
        ExternalSortOperatorDescriptor globalSort = new ExternalSortOperatorDescriptor(spec, maxFrameNumber, keyFields,
                nkmFactory, sortCmpFactories, rdUnnestedMessage, Algorithm.QUICK_SORT);
        ClusterConfig.setLocationConstraint(spec, globalSort);

        /**
         * construct global group-by operator
         */
        RecordDescriptor rdFinal = DataflowUtils.getRecordDescriptorFromKeyValueClasses(vertexIdClass.getName(),
                MsgList.class.getName());
        IClusteredAggregatorDescriptorFactory aggregatorFactoryFinal = DataflowUtils.getAccumulatingAggregatorFactory(
                conf, true, false);
        ClusteredGroupOperatorDescriptor globalGby = new ClusteredGroupOperatorDescriptor(spec, keyFields,
                sortCmpFactories, aggregatorFactoryFinal, rdFinal);
        ClusterConfig.setLocationConstraint(spec, globalGby);

        /**
         * construct the materializing write operator
         */
        MaterializingWriteOperatorDescriptor materialize = new MaterializingWriteOperatorDescriptor(spec, rdFinal);
        ClusterConfig.setLocationConstraint(spec, materialize);

        RuntimeHookOperatorDescriptor postSuperStep = new RuntimeHookOperatorDescriptor(spec,
                new PostSuperStepRuntimeHookFactory(jobId));
        ClusterConfig.setLocationConstraint(spec, postSuperStep);

        /** construct empty sink operator */
        EmptySinkOperatorDescriptor emptySink2 = new EmptySinkOperatorDescriptor(spec);
        ClusterConfig.setLocationConstraint(spec, emptySink2);

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
        int[] fieldPermutation = new int[] { 0, 1 };
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
        spec.connect(new OneToOneConnectorDescriptor(spec), preSuperStep, 0, scanner, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), scanner, 0, globalSort, 0);
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

        spec.connect(new OneToOneConnectorDescriptor(spec), globalSort, 0, globalGby, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), globalGby, 0, materialize, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materialize, 0, postSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), postSuperStep, 0, emptySink2, 0);

        spec.addRoot(terminateWriter);
        spec.addRoot(finalAggregator);
        spec.addRoot(emptySink2);
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
         * construct pre-superstep hook
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
         * construct index join function update operator
         */
        IFileSplitProvider fileSplitProvider = ClusterConfig.getFileSplitProvider(jobId, PRIMARY_INDEX);
        ITypeTraits[] typeTraits = new ITypeTraits[2];
        typeTraits[0] = new TypeTraits(false);
        typeTraits[1] = new TypeTraits(false);
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[2];
        nullWriterFactories[0] = VertexIdNullWriterFactory.INSTANCE;
        nullWriterFactories[1] = MsgListNullWriterFactory.INSTANCE;

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
                getIndexDataflowHelperFactory(), true, nullWriterFactories, inputRdFactory, 5,
                new ComputeUpdateFunctionFactory(confFactory), preHookFactory, null, rdUnnestedMessage, rdDummy,
                rdPartialAggregate, rdInsert, rdDelete);
        ClusterConfig.setLocationConstraint(spec, join);

        /**
         * construct global sort operator
         */
        INormalizedKeyComputerFactory nkmFactory = JobGenUtil.getINormalizedKeyComputerFactory(conf);
        IBinaryComparatorFactory[] sortCmpFactories = new IBinaryComparatorFactory[1];
        sortCmpFactories[0] = JobGenUtil.getIBinaryComparatorFactory(iteration, WritableComparator.get(vertexIdClass)
                .getClass());
        ExternalSortOperatorDescriptor globalSort = new ExternalSortOperatorDescriptor(spec, maxFrameNumber, keyFields,
                nkmFactory, sortCmpFactories, rdUnnestedMessage, Algorithm.QUICK_SORT);
        ClusterConfig.setLocationConstraint(spec, globalSort);

        /**
         * construct global group-by operator
         */
        IClusteredAggregatorDescriptorFactory aggregatorFactoryFinal = DataflowUtils.getAccumulatingAggregatorFactory(
                conf, true, false);
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

        int[] fieldPermutation = new int[] { 0, 1 };
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
        spec.connect(new OneToOneConnectorDescriptor(spec), materializeRead, 0, join, 0);
        spec.connect(new MToNPartitioningConnectorDescriptor(spec, partionFactory), join, 0, globalSort, 0);
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

        spec.connect(new OneToOneConnectorDescriptor(spec), globalSort, 0, globalGby, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), globalGby, 0, materialize, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), materialize, 0, postSuperStep, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), postSuperStep, 0, emptySink, 0);

        spec.addRoot(terminateWriter);
        spec.addRoot(finalAggregator);
        spec.addRoot(emptySink);

        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy(spec));
        spec.setFrameSize(frameSize);
        return spec;
    }

    @Override
    public JobSpecification[] generateCleanup() throws HyracksException {
        JobSpecification[] cleanups = new JobSpecification[1];
        cleanups[0] = this.dropIndex(PRIMARY_INDEX);
        return cleanups;
    }

}