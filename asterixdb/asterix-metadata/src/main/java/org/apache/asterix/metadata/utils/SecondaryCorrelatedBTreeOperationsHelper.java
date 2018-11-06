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
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.LSMSecondaryIndexBulkLoadOperatorDescriptor;
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
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;

/**
 * This class is used to build secondary LSMBTree index for correlated datasets.
 *
 * @author luochen
 *
 */
public class SecondaryCorrelatedBTreeOperationsHelper extends SecondaryCorrelatedTreeIndexOperationsHelper {

    protected SecondaryCorrelatedBTreeOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());

        boolean isOverridingKeyFieldTypes = index.isOverridingKeyFieldTypes();

        assert dataset.getDatasetType() == DatasetType.INTERNAL;

        // only handle internal datasets
        // Create dummy key provider for feeding the primary index scan.
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // Create dummy key provider for feeding the primary index scan.
        IOperatorDescriptor keyProviderOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);

        // Create primary index scan op.
        IOperatorDescriptor primaryScanOp = createPrimaryIndexScanDiskComponentsOp(spec, metadataProvider,
                getTaggedRecordDescriptor(dataset.getPrimaryRecordDescriptor(metadataProvider)));

        // Assign op.
        IOperatorDescriptor sourceOp = primaryScanOp;
        if (isOverridingKeyFieldTypes && !enforcedItemType.equals(itemType)) {
            sourceOp = createCastOp(spec, dataset.getDatasetType(), index.isEnforced());
            spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
        }
        RecordDescriptor taggedSecondaryRecDesc = getTaggedRecordDescriptor(secondaryRecDesc);
        AlgebricksMetaOperatorDescriptor asterixAssignOp =
                createAssignOp(spec, index.getKeyFieldNames().size(), taggedSecondaryRecDesc);

        // Generate compensate tuples for upsert
        IOperatorDescriptor processorOp =
                createTupleProcessorOp(spec, taggedSecondaryRecDesc, getNumSecondaryKeys(), numPrimaryKeys, false);

        ExternalSortOperatorDescriptor sortOp = createSortOp(spec,
                getTaggedSecondaryComparatorFactories(secondaryComparatorFactories), taggedSecondaryRecDesc);

        // Create secondary BTree bulk load op.
        LSMSecondaryIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp =
                createTreeIndexBulkLoadOp(spec, metadataProvider, taggedSecondaryRecDesc,
                        createFieldPermutationForBulkLoadOp(), getNumSecondaryKeys(), numPrimaryKeys, false);

        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { sinkRuntimeFactory }, new RecordDescriptor[] { taggedSecondaryRecDesc });
        metaOp.setSourceLocation(sourceLoc);
        // Connect the operators.
        spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, processorOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), processorOp, 0, sortOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
        spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoadOp, 0, metaOp, 0);
        spec.addRoot(metaOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    @Override
    protected int getNumSecondaryKeys() {
        return index.getKeyFieldNames().size();
    }

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
        int recordColumn = NUM_TAG_FIELDS + numPrimaryKeys;
        boolean isOverridingKeyTypes = index.isOverridingKeyFieldTypes();
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
                    metadataProvider.getFunctionManager(), isOverridingKeyTypes ? enforcedItemType : sourceType,
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
        // Add serializers and comparators for primary index fields.
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numSecondaryKeys + i] = primaryRecDesc.getFields()[i];
            enforcedRecFields[i] = primaryRecDesc.getFields()[i];
            secondaryTypeTraits[numSecondaryKeys + i] = primaryRecDesc.getTypeTraits()[i];
            enforcedTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
            secondaryComparatorFactories[numSecondaryKeys + i] = primaryComparatorFactories[i];
        }

        enforcedRecFields[numPrimaryKeys] = serdeProvider.getSerializerDeserializer(itemType);
        enforcedTypeTraits[numPrimaryKeys] = typeTraitProvider.getTypeTrait(itemType);
        if (dataset.hasMetaPart()) {
            enforcedRecFields[numPrimaryKeys + 1] = serdeProvider.getSerializerDeserializer(metaType);
            enforcedTypeTraits[numPrimaryKeys + 1] = typeTraitProvider.getTypeTrait(metaType);
        }

        if (numFilterFields > 0) {
            secondaryFieldAccessEvalFactories[numSecondaryKeys] =
                    metadataProvider.getDataFormat().getFieldAccessEvaluatorFactory(
                            metadataProvider.getFunctionManager(), itemType, filterFieldName, recordColumn, sourceLoc);
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
}
