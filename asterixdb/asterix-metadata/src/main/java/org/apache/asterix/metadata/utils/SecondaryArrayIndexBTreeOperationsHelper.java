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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.aggreg.SimpleAlgebricksAccumulatingAggregatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.std.UnnestRuntimeFactory;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.group.AbstractAggregatorDescriptorFactory;
import org.apache.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

public class SecondaryArrayIndexBTreeOperationsHelper extends SecondaryTreeIndexOperationsHelper {
    private final int numAtomicSecondaryKeys, numArraySecondaryKeys, numTotalSecondaryKeys;
    private final EvalFactoryAndRecDescStackBuilder evalFactoryAndRecDescStackBuilder =
            new EvalFactoryAndRecDescStackBuilder();

    private final Index.ArrayIndexDetails arrayIndexDetails;
    private final List<List<String>> flattenedFieldNames;
    private final List<IAType> flattenedKeyTypes;
    private final List<List<Boolean>> unnestFlags;

    protected SecondaryArrayIndexBTreeOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
        arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();

        flattenedFieldNames = new ArrayList<>();
        flattenedKeyTypes = new ArrayList<>();
        unnestFlags = new ArrayList<>();
        for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
            if (e.getUnnestList().isEmpty()) {
                flattenedFieldNames.add(e.getProjectList().get(0));
                flattenedKeyTypes.add(e.getTypeList().get(0));
                unnestFlags.add(ArrayIndexUtil.getUnnestFlags(e.getUnnestList(), e.getProjectList().get(0)));

            } else {
                for (int i = 0; i < e.getProjectList().size(); i++) {
                    List<String> project = e.getProjectList().get(i);
                    flattenedFieldNames.add(ArrayIndexUtil.getFlattenedKeyFieldNames(e.getUnnestList(), project));
                    flattenedKeyTypes.add(e.getTypeList().get(i));
                    unnestFlags.add(ArrayIndexUtil.getUnnestFlags(e.getUnnestList(), project));
                }
            }
        }

        int totalSecondaryKeyCount = 0;
        int atomicSecondaryKeyCount = 0;
        for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
            if (e.getUnnestList().isEmpty()) {
                atomicSecondaryKeyCount++;
                totalSecondaryKeyCount++;
            } else {
                totalSecondaryKeyCount += e.getProjectList().size();
            }
        }

        numTotalSecondaryKeys = totalSecondaryKeyCount;
        numAtomicSecondaryKeys = atomicSecondaryKeyCount;
        numArraySecondaryKeys = numTotalSecondaryKeys - numAtomicSecondaryKeys;
    }

    private int findPosOfArrayIndexElement() throws AsterixException {
        for (int i = 0; i < arrayIndexDetails.getElementList().size(); i++) {
            if (!arrayIndexDetails.getElementList().get(i).getUnnestList().isEmpty()) {
                return i;
            }
        }
        throw new AsterixException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc, "No array index found.");
    }

    @Override
    protected void setSecondaryRecDescAndComparators() throws AlgebricksException {
        Index.ArrayIndexDetails arrayIndexDetails = (Index.ArrayIndexDetails) index.getIndexDetails();
        int numSecondaryKeys = this.getNumSecondaryKeys();
        secondaryFieldAccessEvalFactories = new IScalarEvaluatorFactory[numSecondaryKeys + numFilterFields];
        secondaryComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys + numPrimaryKeys];
        secondaryBloomFilterKeyFields = new int[numSecondaryKeys];
        ISerializerDeserializer[] secondaryRecFields =
                new ISerializerDeserializer[numPrimaryKeys + numSecondaryKeys + numFilterFields];
        ISerializerDeserializer[] enforcedRecFields =
                new ISerializerDeserializer[1 + numPrimaryKeys + (dataset.hasMetaPart() ? 1 : 0) + numFilterFields];
        ITypeTraits[] enforcedTypeTraits =
                new ITypeTraits[1 + numPrimaryKeys + (dataset.hasMetaPart() ? 1 : 0) + numFilterFields];
        secondaryTypeTraits = new ITypeTraits[numSecondaryKeys + numPrimaryKeys];
        ISerializerDeserializerProvider serdeProvider = metadataProvider.getDataFormat().getSerdeProvider();
        ITypeTraitProvider typeTraitProvider = metadataProvider.getDataFormat().getTypeTraitProvider();
        IBinaryComparatorFactoryProvider comparatorFactoryProvider =
                metadataProvider.getDataFormat().getBinaryComparatorFactoryProvider();
        boolean isOverridingKeyFieldTypes = arrayIndexDetails.isOverridingKeyFieldTypes();
        int flattenedListPos = 0;
        for (Index.ArrayIndexElement e : arrayIndexDetails.getElementList()) {
            for (int i = 0; i < e.getProjectList().size(); i++) {
                addSKEvalFactories(itemType, flattenedListPos, false, e);
                Pair<IAType, Boolean> keyTypePair = ArrayIndexUtil.getNonNullableOpenFieldType(e.getTypeList().get(i),
                        e.getUnnestList(), e.getProjectList().get(i), itemType);
                IAType keyType = keyTypePair.first;
                anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
                ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(keyType);
                secondaryRecFields[flattenedListPos] = keySerde;
                secondaryComparatorFactories[flattenedListPos] =
                        comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
                secondaryTypeTraits[flattenedListPos] = typeTraitProvider.getTypeTrait(keyType);
                secondaryBloomFilterKeyFields[flattenedListPos] = flattenedListPos;

                flattenedListPos++;
            }
        }
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            // Add serializers and comparators for primary index fields.
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFields[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
                enforcedRecFields[i] = primaryRecDesc.getFields()[i];
                secondaryTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
                enforcedTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
                secondaryComparatorFactories[numSecondaryKeys + i] = primaryComparatorFactories[i];
            }
        } else {
            // Add serializers and comparators for RID fields.
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFields[numSecondaryKeys + i] = IndexingConstants.getSerializerDeserializer(i);
                enforcedRecFields[i] = IndexingConstants.getSerializerDeserializer(i);
                secondaryTypeTraits[numSecondaryKeys + i] = IndexingConstants.getTypeTraits(i);
                enforcedTypeTraits[i] = IndexingConstants.getTypeTraits(i);
                secondaryComparatorFactories[numSecondaryKeys + i] = IndexingConstants.getComparatorFactory(i);
            }
        }
        enforcedRecFields[numPrimaryKeys] = serdeProvider.getSerializerDeserializer(itemType);
        enforcedTypeTraits[numPrimaryKeys] = typeTraitProvider.getTypeTrait(itemType);
        if (dataset.hasMetaPart()) {
            enforcedRecFields[numPrimaryKeys + 1] = serdeProvider.getSerializerDeserializer(metaType);
            enforcedTypeTraits[numPrimaryKeys + 1] = typeTraitProvider.getTypeTrait(metaType);
        }

        if (numFilterFields > 0) {
            ARecordType filterItemType =
                    ((InternalDatasetDetails) dataset.getDatasetDetails()).getFilterSourceIndicator() == 0 ? itemType
                            : metaType;
            addSKEvalFactories(itemType, numSecondaryKeys, true, null);
            Pair<IAType, Boolean> keyTypePair;
            keyTypePair = Index.getNonNullableKeyFieldType(filterFieldName, filterItemType);
            IAType type = keyTypePair.first;
            ISerializerDeserializer serde = serdeProvider.getSerializerDeserializer(type);
            secondaryRecFields[numPrimaryKeys + numSecondaryKeys] = serde;
            enforcedRecFields[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)] = serde;
            enforcedTypeTraits[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)] =
                    typeTraitProvider.getTypeTrait(type);
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields, secondaryTypeTraits);
        enforcedRecDesc = new RecordDescriptor(enforcedRecFields, enforcedTypeTraits);
    }

    @Override
    protected int getNumSecondaryKeys() {
        return arrayIndexDetails.getElementList().stream().map(e -> e.getProjectList().size()).reduce(0, Integer::sum);
    }

    protected int[] createFieldPermutationForBulkLoadOp(int numSecondaryKeyFields) {
        int[] fieldPermutation = new int[numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = i;
        }
        return fieldPermutation;
    }

    protected void addSKEvalFactories(ARecordType recordType, int fieldPos, boolean isFilterField,
            Index.ArrayIndexElement workingElement) throws AlgebricksException {
        if (isFilterField) {
            addFilterFieldToBuilder(recordType);
            return;
        }

        List<String> flattenedFieldName = flattenedFieldNames.get(fieldPos);
        List<Boolean> workingUnnestFlags = unnestFlags.get(fieldPos);
        if (workingUnnestFlags.stream().noneMatch(b -> b)) {
            addAtomicFieldToBuilder(recordType, fieldPos);

        } else {
            EvalFactoryAndRecDescInvoker commandExecutor =
                    new EvalFactoryAndRecDescInvoker(!evalFactoryAndRecDescStackBuilder.isUnnestEvalPopulated());
            ArrayIndexUtil.walkArrayPath(index, workingElement, recordType, flattenedFieldName, workingUnnestFlags,
                    commandExecutor);
        }
    }

    /**
     * The following job spec is produced: (key provider) -> (PIDX scan) -> (cast)? -> (assign)? ->
     * ((unnest) -> (assign))* -> (select)? -> (sort)? -> (bulk load) -> (sink)
     */
    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            throw new UnsupportedOperationException("Array indexes on external datasets not currently supported.");
        } else {
            IndexUtil.bindJobEventListener(spec, metadataProvider);

            // Start the job spec. Create a key provider and connect this to a primary index scan.
            IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
            // if format == column, then project only the indexed fields
            ITupleProjectorFactory projectorFactory =
                    IndexUtil.createPrimaryIndexScanTupleProjectorFactory(dataset.getDatasetFormatInfo(),
                            arrayIndexDetails.getIndexExpectedType(), itemType, metaType, numPrimaryKeys);
            IOperatorDescriptor targetOp =
                    DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset, projectorFactory);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

            sourceOp = targetOp;

            // We do not index meta fields. Project away meta fields if they exist.
            if (dataset.hasMetaPart()) {
                int[] outColumns = new int[] { primaryRecDesc.getFieldCount() };
                int[] projectionList = new int[primaryRecDesc.getFieldCount() - 1];
                for (int i = 0; i < projectionList.length - 1; i++) {
                    projectionList[i] = i;
                }
                projectionList[projectionList.length - 1] = primaryRecDesc.getFieldCount() - 2;
                ISerializerDeserializer[] fields = new ISerializerDeserializer[primaryRecDesc.getFieldCount() - 1];
                ITypeTraits[] typeTraits = new ITypeTraits[primaryRecDesc.getFieldCount() - 1];
                for (int i = 0; i < primaryRecDesc.getFieldCount() - 1; i++) {
                    fields[i] = primaryRecDesc.getFields()[i];
                    typeTraits[i] = primaryRecDesc.getTypeTraits()[i];
                }
                targetOp = createGenericAssignOp(spec, new ArrayList<>(), new RecordDescriptor(fields, typeTraits),
                        outColumns, projectionList);
                spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
                sourceOp = targetOp;
            }

            // Perform the unnest work.
            final Mutable<IOperatorDescriptor> sourceOpRef = new MutableObject<>(sourceOp);
            final Mutable<IOperatorDescriptor> targetOpRef = new MutableObject<>(targetOp);
            LoadingJobBuilder jobBuilder = new LoadingJobBuilder(spec, sourceOpRef, targetOpRef);
            int posOfArrayElement = findPosOfArrayIndexElement();
            ArrayIndexUtil.walkArrayPath(flattenedFieldNames.get(posOfArrayElement), unnestFlags.get(posOfArrayElement),
                    jobBuilder);
            sourceOp = sourceOpRef.getValue();

            if (anySecondaryKeyIsNullable || arrayIndexDetails.isOverridingKeyFieldTypes()) {
                // If any of the secondary fields are nullable, then we need to filter out the nulls.
                List<IAType> secondaryKeyTypes = new ArrayList<>();
                if (arrayIndexDetails.isOverridingKeyFieldTypes() && !enforcedItemType.equals(itemType)) {
                    for (Index.ArrayIndexElement arrayIndexElement : arrayIndexDetails.getElementList()) {
                        List<IAType> typeList = arrayIndexElement.getTypeList();
                        secondaryKeyTypes.addAll(typeList);
                    }
                }
                targetOp = createCastFilterAnyUnknownSelectOp(spec, numTotalSecondaryKeys, secondaryRecDesc,
                        secondaryKeyTypes);
                spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
                sourceOp = targetOp;
            }

            // Sort by secondary keys, then primary keys.
            IBinaryComparatorFactory[] comparatorFactories = getComparatorFactoriesForOrder();
            targetOp = createSortOp(spec, comparatorFactories, secondaryRecDesc);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
            sourceOp = targetOp;

            // Only insert unique <SK, PK> pairs into our index,
            targetOp = createPreSortedDistinctOp(spec, comparatorFactories, secondaryRecDesc);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
            sourceOp = targetOp;

            // Apply the bulk loading operator.
            IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                    metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);
            int[] fieldPermutations = createFieldPermutationForBulkLoadOp(numTotalSecondaryKeys);
            int[] pkFields = createPkFieldPermutationForBulkLoadOp(fieldPermutations, numTotalSecondaryKeys);
            targetOp = createTreeIndexBulkLoadOp(spec, fieldPermutations, dataflowHelperFactory,
                    StorageConstants.DEFAULT_TREE_FILL_FACTOR, pkFields);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

            // Apply the sink.
            sourceOp = targetOp;
            SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
            sinkRuntimeFactory.setSourceLocation(sourceLoc);
            targetOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                    new IPushRuntimeFactory[] { sinkRuntimeFactory }, new RecordDescriptor[] { secondaryRecDesc });
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
            spec.addRoot(targetOp);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
            return spec;
        }
    }

    private IBinaryComparatorFactory[] getComparatorFactoriesForOrder() {
        IBinaryComparatorFactory[] comparatorFactories =
                new IBinaryComparatorFactory[numPrimaryKeys + numTotalSecondaryKeys + numFilterFields];
        if (numTotalSecondaryKeys >= 0) {
            System.arraycopy(secondaryComparatorFactories, 0, comparatorFactories, 0, numTotalSecondaryKeys);
        }
        if (numPrimaryKeys >= 0) {
            System.arraycopy(primaryComparatorFactories, 0, comparatorFactories, numTotalSecondaryKeys, numPrimaryKeys);
        }
        if (numFilterFields > 0) {
            comparatorFactories[numTotalSecondaryKeys + numPrimaryKeys] =
                    secondaryComparatorFactories[numTotalSecondaryKeys];
        }
        return comparatorFactories;
    }

    private IOperatorDescriptor createPreSortedDistinctOp(JobSpecification spec,
            IBinaryComparatorFactory[] secondaryComparatorFactories, RecordDescriptor secondaryRecDesc) {
        int[] distinctFields = new int[secondaryComparatorFactories.length];
        for (int i = 0; i < secondaryComparatorFactories.length; i++) {
            distinctFields[i] = i;
        }

        IAggregateEvaluatorFactory[] aggFactories = new IAggregateEvaluatorFactory[] {};
        AbstractAggregatorDescriptorFactory aggregatorFactory =
                new SimpleAlgebricksAccumulatingAggregatorFactory(aggFactories, distinctFields);
        aggregatorFactory.setSourceLocation(sourceLoc);

        PreclusteredGroupOperatorDescriptor distinctOp = new PreclusteredGroupOperatorDescriptor(spec, distinctFields,
                secondaryComparatorFactories, aggregatorFactory, secondaryRecDesc, false, -1);
        distinctOp.setSourceLocation(sourceLoc);

        return distinctOp;
    }

    /**
     * Create an UNNEST operator for use with array indexes, which will perform the unnest and append the new field to
     * the end of the input tuple. We expect three types of inputs to this operator:
     * <p>
     * <ol>
     * <li>Tuples from a PIDX scan, which are in the format [PKs, record].
     * <li>Tuples from an UNNEST op, which are in the format [PKs, (filter)?, intermediate-record].
     * <li>Tuples from an UNNEST op that has already assigned a composite key, which are in the format:
     * [PKs, (atomic SKs)?, (filter)?, intermediate-record].
     * </ol>
     * <p>
     * In all cases here, the field(s) we want to unnest are located at the end of the input tuple.
     */
    private AlgebricksMetaOperatorDescriptor createUnnestOp(JobSpecification spec, int inputWidth,
            IScalarEvaluatorFactory sef, RecordDescriptor unnestRecDesc) throws AlgebricksException {
        int[] projectionList = IntStream.range(0, inputWidth + 1).toArray();
        IUnnestingEvaluatorFactory unnestingEvaluatorFactory =
                metadataProvider.getFunctionManager().lookupFunction(BuiltinFunctions.SCAN_COLLECTION, sourceLoc)
                        .createUnnestingEvaluatorFactory(new IScalarEvaluatorFactory[] { sef });
        UnnestRuntimeFactory unnest = new UnnestRuntimeFactory(projectionList.length - 1, unnestingEvaluatorFactory,
                projectionList, false, null);
        unnest.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor algebricksMetaOperatorDescriptor = new AlgebricksMetaOperatorDescriptor(spec,
                1, 1, new IPushRuntimeFactory[] { unnest }, new RecordDescriptor[] { unnestRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, algebricksMetaOperatorDescriptor,
                primaryPartitionConstraint);
        return algebricksMetaOperatorDescriptor;
    }

    /**
     * Create an ASSIGN operator for use in-between UNNEST operators. This means that the projected fields will be in
     * the order of [PKs, (atomic SKs)?, (filter)?, intermediate record], from the following expected inputs:
     * <p>
     * <ol>
     * <li>Tuples from an PIDX scan -> UNNEST op, which are in the format [PKs, record, intermediate record].
     * <li>Tuples from an ASSIGN op -> UNNEST op, which are in the format [PKs, (atomic SKs)?, (filter)?, record,
     * intermediate record].
     * <p>
     * </ol>
     * In addition to removing the record filter for the first case, we must also retrieve the filter field and the
     * top-level atomic SK if they exist.
     */
    private AlgebricksMetaOperatorDescriptor createIntermediateAssignOp(JobSpecification spec, boolean isFirstAssign,
            int inputWidth, List<IScalarEvaluatorFactory> sefs, RecordDescriptor assignRecDesc) {
        int[] outColumns, projectionList;
        if (isFirstAssign) {
            projectionList = new int[numPrimaryKeys + numAtomicSecondaryKeys + numFilterFields + 1];
            outColumns = IntStream.range(inputWidth, (numAtomicSecondaryKeys + numFilterFields == 1) ? (inputWidth + 1)
                    : inputWidth + numAtomicSecondaryKeys + numFilterFields).toArray();
            for (int i = 0; i < numPrimaryKeys; i++) {
                projectionList[i] = i;
            }
            System.arraycopy(outColumns, 0, projectionList, numPrimaryKeys, numAtomicSecondaryKeys);
            if (numFilterFields > 0) {
                projectionList[numPrimaryKeys + numAtomicSecondaryKeys] = outColumns[outColumns.length - 1];
            }
        } else {
            outColumns = new int[] { inputWidth };
            projectionList = new int[inputWidth - 1];
            for (int i = 0; i < projectionList.length - 1; i++) {
                projectionList[i] = i;
            }
        }
        projectionList[projectionList.length - 1] = inputWidth - 1;
        return createGenericAssignOp(spec, sefs, assignRecDesc, outColumns, projectionList);
    }

    /**
     * Create an ASSIGN operator for use after the final UNNEST operator for an array index bulk-loading job. This means
     * that the projected fields will be in the order of [SKs, PKs, filter], from the following expected inputs:
     * <p>
     * <ol>
     * <li>Tuples from an PIDX scan -> UNNEST op, which are in the format [PKs, record, intermediate record].
     * <li>Tuples from an ASSIGN op -> UNNEST op, which are in the format [PKs, (atomic SKs)?, (filter)?, record,
     * intermediate record].
     * <p>
     * </ol>
     * For the first case, we must also retrieve the filter field and the top-level atomic SK if they exist.
     */
    private AlgebricksMetaOperatorDescriptor createFinalAssignOp(JobSpecification spec, boolean isFirstAssign,
            int inputWidth, List<IScalarEvaluatorFactory> sefs, RecordDescriptor assignRecDesc) {
        int[] outColumns, projectionList;
        if (isFirstAssign) {
            int outColumnsCursor = 0;
            projectionList = new int[numPrimaryKeys + numTotalSecondaryKeys + numFilterFields];
            outColumns = IntStream.range(inputWidth, (numTotalSecondaryKeys + numFilterFields == 1) ? (inputWidth + 1)
                    : inputWidth + numTotalSecondaryKeys + numFilterFields).toArray();
            for (int i = 0; i < numTotalSecondaryKeys; i++) {
                int sizeOfFieldNamesForI = flattenedFieldNames.get(i).size();
                if (unnestFlags.get(i).get(sizeOfFieldNamesForI - 1)) {
                    projectionList[i] = numPrimaryKeys + 1;
                } else {
                    projectionList[i] = outColumns[outColumnsCursor++];
                }
            }
            for (int i = numTotalSecondaryKeys; i < numPrimaryKeys + numTotalSecondaryKeys; i++) {
                projectionList[i] = i - numTotalSecondaryKeys;
            }
            if (numFilterFields > 0) {
                projectionList[projectionList.length - 1] = outColumns[outColumnsCursor];
            }
        } else {
            int atomicSKCursor = 0, arraySKCursor = 0;
            projectionList = new int[numPrimaryKeys + numTotalSecondaryKeys + numFilterFields];
            outColumns = IntStream.range(inputWidth, inputWidth + numArraySecondaryKeys).toArray();
            for (int i = 0; i < numTotalSecondaryKeys; i++) {
                int sizeOfFieldNamesForI = flattenedFieldNames.get(i).size();
                if (unnestFlags.get(i).stream().noneMatch(b -> b)) {
                    projectionList[i] = numPrimaryKeys + atomicSKCursor++;
                } else if (!unnestFlags.get(i).get(sizeOfFieldNamesForI - 1)) {
                    projectionList[i] = outColumns[arraySKCursor++];
                } else {
                    projectionList[i] = numPrimaryKeys + numAtomicSecondaryKeys + numFilterFields + 1;
                }
            }
            for (int i = 0; i < numPrimaryKeys; i++) {
                projectionList[i + numTotalSecondaryKeys] = i;
            }
            if (numFilterFields > 0) {
                projectionList[numPrimaryKeys + numTotalSecondaryKeys] = numPrimaryKeys + numAtomicSecondaryKeys;
            }
        }
        return createGenericAssignOp(spec, sefs, assignRecDesc, outColumns, projectionList);
    }

    private AlgebricksMetaOperatorDescriptor createGenericAssignOp(JobSpecification spec,
            List<IScalarEvaluatorFactory> sefs, RecordDescriptor assignRecDesc, int[] outColumns,
            int[] projectionList) {
        AssignRuntimeFactory assign =
                new AssignRuntimeFactory(outColumns, sefs.toArray(new IScalarEvaluatorFactory[0]), projectionList);
        assign.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor algebricksMetaOperatorDescriptor = new AlgebricksMetaOperatorDescriptor(spec,
                1, 1, new IPushRuntimeFactory[] { assign }, new RecordDescriptor[] { assignRecDesc });
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, algebricksMetaOperatorDescriptor,
                primaryPartitionConstraint);
        return algebricksMetaOperatorDescriptor;
    }

    private void addAtomicFieldToBuilder(ARecordType recordType, int indexPos) throws AlgebricksException {
        IAType workingType = Index.getNonNullableOpenFieldType(index, flattenedKeyTypes.get(indexPos),
                flattenedFieldNames.get(indexPos), recordType).first;
        IScalarEvaluatorFactory sef =
                metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(metadataProvider.getFunctionManager(),
                        recordType, flattenedFieldNames.get(indexPos), numPrimaryKeys, sourceLoc);
        evalFactoryAndRecDescStackBuilder.addAtomicSK(sef, workingType);
    }

    private void addFilterFieldToBuilder(ARecordType recordType) throws AlgebricksException {
        IScalarEvaluatorFactory sef = metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(
                metadataProvider.getFunctionManager(), recordType, filterFieldName, numPrimaryKeys, sourceLoc);
        evalFactoryAndRecDescStackBuilder.addFilter(sef,
                Index.getNonNullableKeyFieldType(filterFieldName, recordType).first);
    }

    class EvalFactoryAndRecDescInvoker implements ArrayIndexUtil.TypeTrackerCommandExecutor {
        private final boolean isFirstWalk;

        public EvalFactoryAndRecDescInvoker(boolean isFirstWalk) {
            this.isFirstWalk = isFirstWalk;
        }

        @Override
        public void executeActionOnEachArrayStep(ARecordType startingStepRecordType, IAType workingType,
                List<String> fieldName, boolean isFirstArrayStep, boolean isLastUnnestInIntermediateStep)
                throws AlgebricksException {
            if (!this.isFirstWalk) {
                // We have already added the appropriate UNNESTs.
                return;
            }

            int sourceColumnForNestedArrays = numPrimaryKeys + numAtomicSecondaryKeys + numFilterFields;
            int sourceColumnForFirstUnnestInAtomicPath =
                    isFirstArrayStep ? numPrimaryKeys : sourceColumnForNestedArrays;
            IScalarEvaluatorFactory sef = metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(
                    metadataProvider.getFunctionManager(), startingStepRecordType, fieldName,
                    sourceColumnForFirstUnnestInAtomicPath, sourceLoc);
            evalFactoryAndRecDescStackBuilder.addUnnest(sef, workingType);
        }

        @Override
        public void executeActionOnFinalArrayStep(Index.ArrayIndexElement workingElement, ARecordType baseRecordType,
                ARecordType startingStepRecordType, List<String> fieldName, boolean isNonArrayStep,
                boolean requiresOnlyOneUnnest) throws AlgebricksException {
            // If the final value is nested inside a record, add this SEF.
            if (!isNonArrayStep) {
                return;
            }

            int sourceColumnForFinalEvaluator = 1 + ((requiresOnlyOneUnnest) ? numPrimaryKeys
                    : (numPrimaryKeys + numAtomicSecondaryKeys + numFilterFields));
            IScalarEvaluatorFactory sef = metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(
                    metadataProvider.getFunctionManager(), startingStepRecordType, fieldName,
                    sourceColumnForFinalEvaluator, sourceLoc);
            evalFactoryAndRecDescStackBuilder.addFinalArraySK(sef);
        }
    }

    class LoadingJobBuilder implements ArrayIndexUtil.ActionCounterCommandExecutor {
        private final Deque<RecordDescriptor> recDescStack = evalFactoryAndRecDescStackBuilder.buildRecDescStack();
        private final Deque<List<IScalarEvaluatorFactory>> sefStack =
                evalFactoryAndRecDescStackBuilder.buildEvalFactoryStack();

        private final JobSpecification spec;
        private final Mutable<IOperatorDescriptor> sourceOpRef;
        private final Mutable<IOperatorDescriptor> targetOpRef;
        private RecordDescriptor workingRecDesc = recDescStack.pop(), nextRecDesc;

        LoadingJobBuilder(JobSpecification spec, Mutable<IOperatorDescriptor> sourceOpRef,
                Mutable<IOperatorDescriptor> targetOpRef) throws AlgebricksException {
            this.spec = spec;
            this.sourceOpRef = sourceOpRef;
            this.targetOpRef = targetOpRef;
        }

        private void connectAndMoveToNextOp() {
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOpRef.getValue(), 0, targetOpRef.getValue(), 0);
            sourceOpRef.setValue(targetOpRef.getValue());
            workingRecDesc = nextRecDesc;
        }

        @Override
        public void executeActionOnFirstArrayStep() throws AlgebricksException {
            IScalarEvaluatorFactory sef = sefStack.pop().get(0);
            nextRecDesc = recDescStack.pop();
            targetOpRef.setValue(createUnnestOp(spec, workingRecDesc.getFieldCount(), sef, nextRecDesc));
            connectAndMoveToNextOp();
        }

        @Override
        public void executeActionOnIntermediateArrayStep(int numberOfActionsAlreadyPerformed)
                throws AlgebricksException {
            // The purpose of the ASSIGN added here is twofold: 1) is to remove the unnecessary record/list we
            // just unnested, and 2) is to extract the appropriate record fields, if we expect a record next.
            nextRecDesc = recDescStack.pop();
            targetOpRef.setValue(createIntermediateAssignOp(spec, numberOfActionsAlreadyPerformed < 2,
                    workingRecDesc.getFieldCount(), sefStack.pop(), nextRecDesc));
            connectAndMoveToNextOp();

            IScalarEvaluatorFactory sef = sefStack.pop().get(0);
            nextRecDesc = recDescStack.pop();
            targetOpRef.setValue(createUnnestOp(spec, workingRecDesc.getFieldCount(), sef, nextRecDesc));
            connectAndMoveToNextOp();
        }

        @Override
        public void executeActionOnFinalArrayStep(int numberOfActionsAlreadyPerformed) throws AlgebricksException {
            nextRecDesc = recDescStack.pop();
            targetOpRef.setValue(createFinalAssignOp(spec, numberOfActionsAlreadyPerformed < 2,
                    workingRecDesc.getFieldCount(), sefStack.pop(), nextRecDesc));
            connectAndMoveToNextOp();
        }
    }

    class EvalFactoryAndRecDescStackBuilder {
        class EvalFactoryAndPosition {
            final IScalarEvaluatorFactory scalarEvaluatorFactory;
            final int position;

            EvalFactoryAndPosition(IScalarEvaluatorFactory scalarEvaluatorFactory) {
                this.scalarEvaluatorFactory = scalarEvaluatorFactory;
                this.position = workingPosition++;
            }
        }

        private final Deque<EvalFactoryAndPosition> unnestEvalFactories = new ArrayDeque<>();
        private final List<EvalFactoryAndPosition> atomicSKEvalFactories = new ArrayList<>();
        private final List<EvalFactoryAndPosition> finalArraySKEvalFactories = new ArrayList<>();
        private final Queue<IAType> unnestEvalTypes = new LinkedList<>();
        private final List<IAType> atomicSKEvalTypes = new ArrayList<>();
        private EvalFactoryAndPosition filterEvalFactory = null;
        private IAType filterEvalType = null;
        private int workingPosition = 0;

        public void addAtomicSK(IScalarEvaluatorFactory sef, IAType type) {
            atomicSKEvalFactories.add(new EvalFactoryAndPosition(sef));
            atomicSKEvalTypes.add(type);
        }

        public void addFilter(IScalarEvaluatorFactory sef, IAType type) {
            filterEvalFactory = new EvalFactoryAndPosition(sef);
            filterEvalType = type;
        }

        public void addFinalArraySK(IScalarEvaluatorFactory sef) {
            finalArraySKEvalFactories.add(new EvalFactoryAndPosition(sef));
        }

        public void addUnnest(IScalarEvaluatorFactory sef, IAType type) {
            unnestEvalFactories.push(new EvalFactoryAndPosition(sef));
            unnestEvalTypes.add(type);
        }

        public boolean isUnnestEvalPopulated() {
            return !unnestEvalFactories.isEmpty();
        }

        /**
         * Order our scalar evaluator factory stack in the order each UNNEST and ASSIGN op will be performed.
         * <p>
         * Visually, our stack looks like:
         *
         * <pre>
         *  [ first UNNEST SEF ------------------------------------------------- ]
         *  [ first ASSIGN SEFs -- atomic SKs and filter  ---------------------- ]
         * *[ any intermediate UNNEST SEFs --column accessors / record accessors ]
         *  [ final ASSIGN SEFs -- array SKs (record accessors) ---------------- ]
         * </pre>
         */
        public Deque<List<IScalarEvaluatorFactory>> buildEvalFactoryStack() {
            Deque<List<EvalFactoryAndPosition>> resultant = new ArrayDeque<>();
            resultant.push(finalArraySKEvalFactories);
            int initialUnnestEvalFactorySize = unnestEvalFactories.size();
            for (int i = 0; i < initialUnnestEvalFactorySize - 1; i++) {
                if (i != 0) {
                    resultant.push(new ArrayList<>());
                }
                resultant.push(Collections.singletonList(unnestEvalFactories.pop()));
                if (i == initialUnnestEvalFactorySize - 2) {
                    resultant.push(new ArrayList<>());
                }
            }

            // Sort the SEFs according to the index order.
            resultant.peek().addAll(atomicSKEvalFactories);
            List<EvalFactoryAndPosition> reorderedSEFs = new ArrayList<>(Objects.requireNonNull(resultant.peek()));
            reorderedSEFs.sort(Comparator.comparingInt(s -> s.position));
            resultant.pop();
            resultant.push(reorderedSEFs);

            // Append our filter eval factory last.
            if (filterEvalFactory != null) {
                resultant.peek().add(filterEvalFactory);
            }
            resultant.push(Collections.singletonList(unnestEvalFactories.pop()));
            return resultant.stream()
                    .map(l -> l.stream().map(s -> s.scalarEvaluatorFactory).collect(Collectors.toList()))
                    .collect(Collectors.toCollection(ArrayDeque::new));
        }

        /**
         * Order our record descriptor stack in the same order as our SEF stack.
         * <p>
         * Visually, our stack looks like:
         *
         * <pre>
         *  [ primary record descriptor --------------------------------------- ]
         *  [ primary record descriptor w/ first UNNESTed field at the end ---- ]
         *  [ record descriptor w/ atomic fields, w/o record, w/ UNNESTed field ]
         * *[ same record descriptor as above, w/ new unnested field ---------- ]
         * *[ same record descriptor as above, w/o record field --------------- ]
         *  [ secondary record descriptor ------------------------------------- ]
         * </pre>
         */
        public Deque<RecordDescriptor> buildRecDescStack() throws AlgebricksException {
            int initialUnnestEvalTypesSize = unnestEvalTypes.size();
            Deque<RecordDescriptor> resultant = new ArrayDeque<>();
            RecordDescriptor recDescBeforeFirstUnnest = primaryRecDesc;
            if (dataset.hasMetaPart()) {
                ISerializerDeserializer[] fields = new ISerializerDeserializer[primaryRecDesc.getFieldCount() - 1];
                ITypeTraits[] typeTraits = new ITypeTraits[primaryRecDesc.getFieldCount() - 1];
                for (int i = 0; i < primaryRecDesc.getFieldCount() - 1; i++) {
                    fields[i] = primaryRecDesc.getFields()[i];
                    typeTraits[i] = primaryRecDesc.getTypeTraits()[i];
                }
                recDescBeforeFirstUnnest = new RecordDescriptor(fields, typeTraits);
            }
            resultant.addLast(recDescBeforeFirstUnnest);
            resultant.addLast(createUnnestRecDesc(recDescBeforeFirstUnnest, unnestEvalTypes.remove()));
            for (int i = 0; i < initialUnnestEvalTypesSize - 1; i++) {
                resultant.addLast(createAssignRecDesc(resultant.getLast(), i == 0));
                resultant.addLast(createUnnestRecDesc(resultant.getLast(), unnestEvalTypes.remove()));
            }
            resultant.addLast(secondaryRecDesc);
            return resultant;
        }

        private RecordDescriptor createUnnestRecDesc(RecordDescriptor priorRecDesc, IAType type)
                throws AlgebricksException {
            ISerializerDeserializerProvider serdeProvider = metadataProvider.getDataFormat().getSerdeProvider();
            ISerializerDeserializer[] unnestFields = Stream
                    .concat(Stream.of(priorRecDesc.getFields()),
                            Stream.of(serdeProvider.getSerializerDeserializer(type)))
                    .toArray(ISerializerDeserializer[]::new);
            ITypeTraits[] unnestTypes = Stream.concat(Stream.of(priorRecDesc.getTypeTraits()),
                    Stream.of(TypeTraitProvider.INSTANCE.getTypeTrait(type))).toArray(ITypeTraits[]::new);
            return new RecordDescriptor(unnestFields, unnestTypes);
        }

        private RecordDescriptor createAssignRecDesc(RecordDescriptor priorRecDesc, boolean isFirstAssign)
                throws AlgebricksException {
            ArrayList<ISerializerDeserializer> assignFields = new ArrayList<>();
            ArrayList<ITypeTraits> assignTypes = new ArrayList<>();
            if (isFirstAssign) {
                ISerializerDeserializerProvider serdeProvider = metadataProvider.getDataFormat().getSerdeProvider();
                for (int i = 0; i < numPrimaryKeys; i++) {
                    assignFields.add(priorRecDesc.getFields()[i]);
                    assignTypes.add(priorRecDesc.getTypeTraits()[i]);
                }
                for (IAType s : atomicSKEvalTypes) {
                    assignFields.add(serdeProvider.getSerializerDeserializer(s));
                    assignTypes.add(TypeTraitProvider.INSTANCE.getTypeTrait(s));
                }
                if (filterEvalType != null) {
                    assignFields.add(serdeProvider.getSerializerDeserializer(filterEvalType));
                    assignTypes.add(TypeTraitProvider.INSTANCE.getTypeTrait(filterEvalType));
                }
                assignFields.add(priorRecDesc.getFields()[priorRecDesc.getFieldCount() - 1]);
                assignTypes.add(priorRecDesc.getTypeTraits()[priorRecDesc.getFieldCount() - 1]);
            } else {
                assignFields = new ArrayList<>(Arrays.asList(priorRecDesc.getFields()));
                assignTypes = new ArrayList<>(Arrays.asList(priorRecDesc.getTypeTraits()));
                assignFields.remove(priorRecDesc.getFieldCount() - 2);
                assignTypes.remove(priorRecDesc.getFieldCount() - 2);
            }
            return new RecordDescriptor(assignFields.toArray(new ISerializerDeserializer[0]),
                    assignTypes.toArray(new ITypeTraits[0]));
        }
    }
}
