/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.metadata.utils;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.context.TransactionSubsystemProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.dataflow.data.nontagged.MissingWriterFactory;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.runtime.operators.LSMSecondaryIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.operators.LSMSecondaryIndexCreationTupleProcessorOperatorDescriptor;
import org.apache.asterix.transaction.management.opcallbacks.PrimaryIndexInstantSearchOperationCallbackFactory;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.dataflow.LSMBTreeDiskComponentScanOperatorDescriptor;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * This class is used to build secondary LSM index for correlated datasets.
 *
 * @author luochen
 *
 */
public abstract class SecondaryCorrelatedTreeIndexOperationsHelper extends SecondaryTreeIndexOperationsHelper {

    protected final static int COMPONENT_POS_OFFSET = 0;

    protected final static int ANTI_MATTER_OFFSET = 1;

    protected final static int NUM_TAG_FIELDS = 2;

    /**
     * Make sure tuples are in the descending order w.r.t. component_pos.
     * In the disk component list of an index, components are ordered from newest to oldest.
     * This descending order ensures older components can be bulk loaded first and get a smaller (older)
     * component file timestamp.
     */
    protected static final IBinaryComparatorFactory COMPONENT_POS_COMPARATOR_FACTORY =
            new ComponentPosComparatorFactory();

    public static final class ComponentPosComparatorFactory implements IBinaryComparatorFactory {

        private static final long serialVersionUID = 1L;

        @Override
        public IBinaryComparator createBinaryComparator() {
            return new IBinaryComparator() {
                final IBinaryComparator comparator =
                        BinaryComparatorFactoryProvider.INTEGER_POINTABLE_INSTANCE.createBinaryComparator();

                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) throws HyracksDataException {
                    return -comparator.compare(b1, s1, l1, b2, s2, l2);
                }
            };
        }

        @Override
        public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
            return registry.getClassIdentifier(getClass(), serialVersionUID);
        }

        @SuppressWarnings("squid:S1172") // unused parameter
        public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json) {
            return COMPONENT_POS_COMPARATOR_FACTORY;
        }
    }

    protected SecondaryCorrelatedTreeIndexOperationsHelper(Dataset dataset, Index index,
            MetadataProvider metadataProvider, SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    protected RecordDescriptor getTaggedRecordDescriptor(RecordDescriptor recDescriptor) {
        ISerializerDeserializer[] fields =
                new ISerializerDeserializer[recDescriptor.getFields().length + NUM_TAG_FIELDS];
        ITypeTraits[] traits = null;
        if (recDescriptor.getTypeTraits() != null) {
            traits = new ITypeTraits[recDescriptor.getTypeTraits().length + NUM_TAG_FIELDS];
        }

        //component position field
        fields[COMPONENT_POS_OFFSET] = IntegerSerializerDeserializer.INSTANCE;
        if (traits != null) {
            traits[COMPONENT_POS_OFFSET] = IntegerPointable.TYPE_TRAITS;
        }

        //anti-matter field
        fields[ANTI_MATTER_OFFSET] = BooleanSerializerDeserializer.INSTANCE;
        if (traits != null) {
            traits[ANTI_MATTER_OFFSET] = BooleanPointable.TYPE_TRAITS;
        }

        for (int i = NUM_TAG_FIELDS; i < fields.length; i++) {
            fields[i] = recDescriptor.getFields()[i - NUM_TAG_FIELDS];
            if (traits != null && i < traits.length) {
                traits[i] = recDescriptor.getTypeTraits()[i - NUM_TAG_FIELDS];
            }
        }

        return new RecordDescriptor(fields, traits);
    }

    protected IBinaryComparatorFactory[] getTaggedSecondaryComparatorFactories(
            IBinaryComparatorFactory[] secondaryComparatorFactories) {
        IBinaryComparatorFactory[] resultFactories =
                new IBinaryComparatorFactory[secondaryComparatorFactories.length + 1];

        // order component ids from largest (oldest) to smallest (newest)
        // this is necessary since during bulk-loading, we need to create older components first
        resultFactories[COMPONENT_POS_OFFSET] = COMPONENT_POS_COMPARATOR_FACTORY;

        for (int i = 1; i < resultFactories.length; i++) {
            resultFactories[i] = secondaryComparatorFactories[i - 1];
        }

        return resultFactories;
    }

    @Override
    protected AlgebricksMetaOperatorDescriptor createCastOp(JobSpecification spec, DatasetType dsType,
            boolean strictCast) throws AlgebricksException {
        int[] outColumns = new int[1];

        // tags(2) + primary keys + record + meta part(?)
        int[] projectionList = new int[NUM_TAG_FIELDS + (dataset.hasMetaPart() ? 2 : 1) + numPrimaryKeys];
        int recordIdx = NUM_TAG_FIELDS + numPrimaryKeys;

        //here we only consider internal dataset
        assert dsType == DatasetType.INTERNAL;

        outColumns[0] = NUM_TAG_FIELDS + numPrimaryKeys;

        int projCount = 0;
        for (int i = 0; i < NUM_TAG_FIELDS; i++) {
            projectionList[projCount++] = i;
        }

        //set primary keys and the record
        for (int i = 0; i <= numPrimaryKeys; i++) {
            projectionList[projCount++] = NUM_TAG_FIELDS + i;
        }
        if (dataset.hasMetaPart()) {
            projectionList[NUM_TAG_FIELDS + numPrimaryKeys + 1] = NUM_TAG_FIELDS + numPrimaryKeys + 1;
        }
        IScalarEvaluatorFactory[] castEvalFact =
                new IScalarEvaluatorFactory[] { new ColumnAccessEvalFactory(recordIdx) };
        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[1];
        sefs[0] = createCastFunction(strictCast, sourceLoc).createEvaluatorFactory(castEvalFact);
        AssignRuntimeFactory castAssign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        castAssign.setSourceLocation(sourceLoc);
        return new AlgebricksMetaOperatorDescriptor(spec, 1, 1, new IPushRuntimeFactory[] { castAssign },
                new RecordDescriptor[] { getTaggedRecordDescriptor(enforcedRecDesc) });
    }

    /**
     * It differs from its base class in that we need to add the extra tags to the fields.
     */
    @Override
    protected AlgebricksMetaOperatorDescriptor createAssignOp(JobSpecification spec, int numSecondaryKeyFields,
            RecordDescriptor secondaryRecDesc) throws AlgebricksException {
        int[] outColumns = new int[numSecondaryKeyFields + numFilterFields];
        int[] projectionList = new int[NUM_TAG_FIELDS + numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < numSecondaryKeyFields + numFilterFields; i++) {
            outColumns[i] = NUM_TAG_FIELDS + numPrimaryKeys + i;
        }
        int projCount = 0;

        //set tag fields
        for (int i = 0; i < NUM_TAG_FIELDS; i++) {
            projectionList[projCount++] = i;
        }

        //set secondary keys
        for (int i = 0; i < numSecondaryKeyFields; i++) {
            projectionList[projCount++] = NUM_TAG_FIELDS + numPrimaryKeys + i;
        }

        //set primary keys
        for (int i = 0; i < numPrimaryKeys; i++) {
            projectionList[projCount++] = NUM_TAG_FIELDS + i;
        }

        //set filter fields
        if (numFilterFields > 0) {
            projectionList[projCount] = NUM_TAG_FIELDS + numPrimaryKeys + numSecondaryKeyFields;
        }

        IScalarEvaluatorFactory[] sefs = new IScalarEvaluatorFactory[secondaryFieldAccessEvalFactories.length];
        for (int i = 0; i < secondaryFieldAccessEvalFactories.length; ++i) {
            sefs[i] = secondaryFieldAccessEvalFactories[i];
        }
        AssignRuntimeFactory assign = new AssignRuntimeFactory(outColumns, sefs, projectionList);
        assign.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor asterixAssignOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 1,
                new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { secondaryRecDesc });
        asterixAssignOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, asterixAssignOp,
                primaryPartitionConstraint);
        return asterixAssignOp;
    }

    protected IOperatorDescriptor createTupleProcessorOp(JobSpecification spec, RecordDescriptor taggedSecondaryRecDesc,
            int numSecondaryKeyFields, int numPrimaryKeyFields, boolean hasBuddyBTree) {
        IOperatorDescriptor op = new LSMSecondaryIndexCreationTupleProcessorOperatorDescriptor(spec,
                taggedSecondaryRecDesc, MissingWriterFactory.INSTANCE, NUM_TAG_FIELDS, numSecondaryKeyFields,
                numPrimaryKeyFields, hasBuddyBTree);
        op.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, op, primaryPartitionConstraint);
        return op;
    }

    @Override
    protected ExternalSortOperatorDescriptor createSortOp(JobSpecification spec,
            IBinaryComparatorFactory[] taggedSecondaryComparatorFactories, RecordDescriptor taggedSecondaryRecDesc) {
        int[] taggedSortFields = new int[taggedSecondaryComparatorFactories.length];
        //component pos
        taggedSortFields[COMPONENT_POS_OFFSET] = COMPONENT_POS_OFFSET;
        //not sorting on anti-matter field
        for (int i = 1; i < taggedSortFields.length; i++) {
            taggedSortFields[i] = i + 1;
        }
        ExternalSortOperatorDescriptor sortOp = new ExternalSortOperatorDescriptor(spec, sortNumFrames,
                taggedSortFields, taggedSecondaryComparatorFactories, taggedSecondaryRecDesc);
        sortOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, sortOp, primaryPartitionConstraint);
        return sortOp;
    }

    protected LSMSecondaryIndexBulkLoadOperatorDescriptor createTreeIndexBulkLoadOp(JobSpecification spec,
            MetadataProvider metadataProvider, RecordDescriptor taggedSecondaryRecDesc, int[] fieldPermutation,
            int numSecondaryKeys, int numPrimaryKeys, boolean hasBuddyBtree) throws AlgebricksException {
        IndexDataflowHelperFactory primaryIndexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), primaryFileSplitProvider);

        IndexDataflowHelperFactory secondaryIndexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);

        LSMSecondaryIndexBulkLoadOperatorDescriptor treeIndexBulkLoadOp =
                new LSMSecondaryIndexBulkLoadOperatorDescriptor(spec, taggedSecondaryRecDesc, primaryIndexHelperFactory,
                        secondaryIndexHelperFactory, fieldPermutation, NUM_TAG_FIELDS, numSecondaryKeys, numPrimaryKeys,
                        hasBuddyBtree);
        treeIndexBulkLoadOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, treeIndexBulkLoadOp,
                secondaryPartitionConstraint);
        return treeIndexBulkLoadOp;
    }

    protected IOperatorDescriptor createPrimaryIndexScanDiskComponentsOp(JobSpecification spec,
            MetadataProvider metadataProvider, RecordDescriptor outRecDesc) {
        ITransactionSubsystemProvider txnSubsystemProvider = TransactionSubsystemProvider.INSTANCE;
        ISearchOperationCallbackFactory searchCallbackFactory = new PrimaryIndexInstantSearchOperationCallbackFactory(
                dataset.getDatasetId(), dataset.getPrimaryBloomFilterFields(), txnSubsystemProvider,
                IRecoveryManager.ResourceType.LSM_BTREE);
        IndexDataflowHelperFactory indexHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), primaryFileSplitProvider);
        LSMBTreeDiskComponentScanOperatorDescriptor primaryScanOp = new LSMBTreeDiskComponentScanOperatorDescriptor(
                spec, outRecDesc, indexHelperFactory, searchCallbackFactory);
        primaryScanOp.setSourceLocation(sourceLoc);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, primaryScanOp,
                primaryPartitionConstraint);
        return primaryScanOp;
    }

    public static SecondaryIndexOperationsHelper createIndexOperationsHelper(Dataset dataset, Index index,
            MetadataProvider metadataProvider, SourceLocation sourceLoc) throws AlgebricksException {

        SecondaryIndexOperationsHelper indexOperationsHelper;
        switch (index.getIndexType()) {
            case BTREE:
                indexOperationsHelper =
                        new SecondaryCorrelatedBTreeOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            case RTREE:
                indexOperationsHelper =
                        new SecondaryCorrelatedRTreeOperationsHelper(dataset, index, metadataProvider, sourceLoc);
                break;
            case SINGLE_PARTITION_WORD_INVIX:
            case SINGLE_PARTITION_NGRAM_INVIX:
            case LENGTH_PARTITIONED_WORD_INVIX:
            case LENGTH_PARTITIONED_NGRAM_INVIX:
                indexOperationsHelper = new SecondaryCorrelatedInvertedIndexOperationsHelper(dataset, index,
                        metadataProvider, sourceLoc);
                break;
            default:
                throw new CompilationException(ErrorCode.COMPILATION_UNKNOWN_INDEX_TYPE, sourceLoc,
                        index.getIndexType());
        }
        indexOperationsHelper.init();
        return indexOperationsHelper;
    }

    protected int[] createFieldPermutationForBulkLoadOp() {
        int[] fieldPermutation = new int[NUM_TAG_FIELDS + getNumSecondaryKeys() + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = i;
        }
        return fieldPermutation;
    }

}
