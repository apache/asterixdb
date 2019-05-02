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

import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.external.operators.ExternalScanOperatorDescriptor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ISerializerDeserializerProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;

public class SecondaryBTreeOperationsHelper extends SecondaryTreeIndexOperationsHelper {

    protected SecondaryBTreeOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        boolean isOverridingKeyFieldTypes = index.isOverridingKeyFieldTypes();
        int[] fieldPermutation = createFieldPermutationForBulkLoadOp(index.getKeyFieldNames().size());
        IIndexDataflowHelperFactory dataflowHelperFactory = new IndexDataflowHelperFactory(
                metadataProvider.getStorageComponentProvider().getStorageManager(), secondaryFileSplitProvider);
        if (dataset.getDatasetType() == DatasetType.EXTERNAL) {
            /*
             * In case of external data,
             * this method is used to build loading jobs for both initial load on index creation
             * and transaction load on dataset referesh
             */

            // Create external indexing scan operator
            ExternalScanOperatorDescriptor primaryScanOp = createExternalIndexingOp(spec);

            // Assign op.
            AbstractOperatorDescriptor sourceOp = primaryScanOp;
            if (isOverridingKeyFieldTypes && !enforcedItemType.equals(itemType)) {
                sourceOp = createCastOp(spec, dataset.getDatasetType(), index.isEnforced());
                spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
            }
            AlgebricksMetaOperatorDescriptor asterixAssignOp =
                    createExternalAssignOp(spec, index.getKeyFieldNames().size(), secondaryRecDesc);

            // If any of the secondary fields are nullable, then add a select op that filters nulls.
            AlgebricksMetaOperatorDescriptor selectOp = null;
            if (anySecondaryKeyIsNullable || isOverridingKeyFieldTypes) {
                selectOp = createFilterNullsSelectOp(spec, index.getKeyFieldNames().size(), secondaryRecDesc);
            }

            // Sort by secondary keys.
            ExternalSortOperatorDescriptor sortOp = createSortOp(spec, secondaryComparatorFactories, secondaryRecDesc);
            // Create secondary BTree bulk load op.
            AbstractSingleActivityOperatorDescriptor secondaryBulkLoadOp;
            IOperatorDescriptor root;
            if (externalFiles != null) {
                // Transaction load
                secondaryBulkLoadOp = createExternalIndexBulkModifyOp(spec, fieldPermutation, dataflowHelperFactory,
                        StorageConstants.DEFAULT_TREE_FILL_FACTOR);
            } else {
                // Initial load
                secondaryBulkLoadOp = createExternalIndexBulkLoadOp(spec, fieldPermutation, dataflowHelperFactory,
                        StorageConstants.DEFAULT_TREE_FILL_FACTOR);
            }
            SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
            sinkRuntimeFactory.setSourceLocation(sourceLoc);
            AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                    new IPushRuntimeFactory[] { sinkRuntimeFactory }, new RecordDescriptor[] { secondaryRecDesc });
            metaOp.setSourceLocation(sourceLoc);
            spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoadOp, 0, metaOp, 0);
            root = metaOp;
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
            if (anySecondaryKeyIsNullable || isOverridingKeyFieldTypes) {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, sortOp, 0);
            } else {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
            }
            spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
            spec.addRoot(root);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
            return spec;
        } else {
            // job spec:
            // key provider -> primary idx -> (cast assign)? -> assign -> (select)? -> (sort)? -> bulk load -> sink
            IndexUtil.bindJobEventListener(spec, metadataProvider);

            // dummy key provider ----> primary index scan
            IOperatorDescriptor sourceOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
            IOperatorDescriptor targetOp = DatasetUtil.createPrimaryIndexScanOp(spec, metadataProvider, dataset);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

            sourceOp = targetOp;
            if (isOverridingKeyFieldTypes && !enforcedItemType.equals(itemType)) {
                // primary index scan ----> cast assign
                targetOp = createCastOp(spec, dataset.getDatasetType(), index.isEnforced());
                spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
                sourceOp = targetOp;
            }
            // primary index OR cast assign ----> assign op
            targetOp = createAssignOp(spec, index.getKeyFieldNames().size(), secondaryRecDesc);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

            sourceOp = targetOp;
            if (anySecondaryKeyIsNullable || isOverridingKeyFieldTypes) {
                // if any of the secondary fields are nullable, then add a select op that filters nulls.
                // assign op ----> select op
                targetOp = createFilterNullsSelectOp(spec, index.getKeyFieldNames().size(), secondaryRecDesc);
                spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
                sourceOp = targetOp;
            }

            // no need to sort if the index is secondary primary index
            if (!index.getKeyFieldNames().isEmpty()) {
                // sort by secondary keys.
                // assign op OR select op ----> sort op
                targetOp = createSortOp(spec, secondaryComparatorFactories, secondaryRecDesc);
                spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);
                sourceOp = targetOp;
            }

            // assign op OR select op OR sort op ----> bulk load op
            targetOp = createTreeIndexBulkLoadOp(spec, fieldPermutation, dataflowHelperFactory,
                    StorageConstants.DEFAULT_TREE_FILL_FACTOR);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, targetOp, 0);

            // bulk load op ----> sink op
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

    @Override
    protected int getNumSecondaryKeys() {
        return index.getKeyFieldNames().size();
    }

    /**
     *      ======
     *     |  SK  |             Bloom filter
     *      ======
     *      ====== ======
     *     |  SK  |  PK  |      comparators, type traits
     *      ====== ======
     *      ====== ........
     *     |  SK  | Filter |    field access evaluators
     *      ====== ........
     *      ====== ====== ........
     *     |  SK  |  PK  | Filter |   record fields
     *      ====== ====== ........
     *      ====== ========= ........ ........
     *     |  PK  | Payload |  Meta  | Filter | enforced record
     *      ====== ========= ........ ........
     */
    @Override
    protected void setSecondaryRecDescAndComparators() throws AlgebricksException {
        int numSecondaryKeys = index.getKeyFieldNames().size();
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
        // Record column is 0 for external datasets, numPrimaryKeys for internal ones
        int recordColumn = dataset.getDatasetType() == DatasetType.INTERNAL ? numPrimaryKeys : 0;
        boolean isOverridingKeyFieldTypes = index.isOverridingKeyFieldTypes();
        for (int i = 0; i < numSecondaryKeys; i++) {
            ARecordType sourceType;
            int sourceColumn;
            List<Integer> keySourceIndicators = index.getKeyFieldSourceIndicators();
            if (keySourceIndicators == null || keySourceIndicators.get(i) == 0) {
                sourceType = itemType;
                sourceColumn = recordColumn;
            } else {
                sourceType = metaType;
                sourceColumn = recordColumn + 1;
            }
            secondaryFieldAccessEvalFactories[i] = metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(
                    metadataProvider.getFunctionManager(), isOverridingKeyFieldTypes ? enforcedItemType : sourceType,
                    index.getKeyFieldNames().get(i), sourceColumn, sourceLoc);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(i),
                    index.getKeyFieldNames().get(i), sourceType);
            IAType keyType = keyTypePair.first;
            anySecondaryKeyIsNullable = anySecondaryKeyIsNullable || keyTypePair.second;
            ISerializerDeserializer keySerde = serdeProvider.getSerializerDeserializer(keyType);
            secondaryRecFields[i] = keySerde;
            secondaryComparatorFactories[i] = comparatorFactoryProvider.getBinaryComparatorFactory(keyType, true);
            secondaryTypeTraits[i] = typeTraitProvider.getTypeTrait(keyType);
            secondaryBloomFilterKeyFields[i] = i;
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
            secondaryFieldAccessEvalFactories[numSecondaryKeys] = metadataProvider.getDataFormat()
                    .getFieldAccessEvaluatorFactory(metadataProvider.getFunctionManager(), itemType, filterFieldName,
                            numPrimaryKeys, sourceLoc);
            Pair<IAType, Boolean> keyTypePair = Index.getNonNullableKeyFieldType(filterFieldName, itemType);
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

    private int[] createFieldPermutationForBulkLoadOp(int numSecondaryKeyFields) {
        int[] fieldPermutation = new int[numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
        for (int i = 0; i < fieldPermutation.length; i++) {
            fieldPermutation[i] = i;
        }
        return fieldPermutation;
    }
}
