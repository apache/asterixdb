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
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.runtime.operators.LSMSecondaryIndexBulkLoadOperatorDescriptor;
import org.apache.asterix.runtime.utils.RuntimeUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
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
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;

public class SecondaryCorrelatedRTreeOperationsHelper extends SecondaryCorrelatedTreeIndexOperationsHelper {

    protected IPrimitiveValueProviderFactory[] valueProviderFactories;
    protected int numNestedSecondaryKeyFields;
    protected ATypeTag keyType;
    protected int[] primaryKeyFields;
    protected int[] rtreeFields;
    protected boolean isPointMBR;
    protected RecordDescriptor secondaryRecDescForPointMBR = null;

    protected SecondaryCorrelatedRTreeOperationsHelper(Dataset dataset, Index index, MetadataProvider metadataProvider,
            SourceLocation sourceLoc) throws AlgebricksException {
        super(dataset, index, metadataProvider, sourceLoc);
    }

    @Override
    protected int getNumSecondaryKeys() {
        return numNestedSecondaryKeyFields;
    }

    @Override
    protected void setSecondaryRecDescAndComparators() throws AlgebricksException {
        List<List<String>> secondaryKeyFields = index.getKeyFieldNames();
        int numSecondaryKeys = secondaryKeyFields.size();
        boolean isOverridingKeyFieldTypes = index.isOverridingKeyFieldTypes();
        if (numSecondaryKeys != 1) {
            throw AsterixException.create(ErrorCode.INDEX_RTREE_MULTIPLE_FIELDS_NOT_ALLOWED, sourceLoc,
                    numSecondaryKeys);
        }
        Pair<IAType, Boolean> spatialTypePair =
                Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0), secondaryKeyFields.get(0), itemType);
        IAType spatialType = spatialTypePair.first;
        anySecondaryKeyIsNullable = spatialTypePair.second;
        isPointMBR = spatialType.getTypeTag() == ATypeTag.POINT || spatialType.getTypeTag() == ATypeTag.POINT3D;
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        numNestedSecondaryKeyFields = numDimensions * 2;
        int recordColumn = NUM_TAG_FIELDS + numPrimaryKeys;
        secondaryFieldAccessEvalFactories = metadataProvider.getDataFormat().createMBRFactory(
                metadataProvider.getFunctionManager(), isOverridingKeyFieldTypes ? enforcedItemType : itemType,
                secondaryKeyFields.get(0), recordColumn, numDimensions, filterFieldName, isPointMBR, sourceLoc);
        secondaryComparatorFactories = new IBinaryComparatorFactory[numNestedSecondaryKeyFields];
        valueProviderFactories = new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];
        ISerializerDeserializer[] secondaryRecFields =
                new ISerializerDeserializer[numPrimaryKeys + numNestedSecondaryKeyFields + numFilterFields];
        ISerializerDeserializer[] enforcedRecFields = new ISerializerDeserializer[1 + numPrimaryKeys + numFilterFields];
        secondaryTypeTraits = new ITypeTraits[numNestedSecondaryKeyFields + numPrimaryKeys];
        ITypeTraits[] enforcedTypeTraits = new ITypeTraits[1 + numPrimaryKeys];
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
        keyType = nestedKeyType.getTypeTag();
        for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
            ISerializerDeserializer keySerde =
                    SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(nestedKeyType);
            secondaryRecFields[i] = keySerde;
            secondaryComparatorFactories[i] =
                    BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(nestedKeyType, true);
            secondaryTypeTraits[i] = TypeTraitProvider.INSTANCE.getTypeTrait(nestedKeyType);
            valueProviderFactories[i] =
                    metadataProvider.getStorageComponentProvider().getPrimitiveValueProviderFactory();

        }
        // Add serializers and comparators for primary index fields.
        // only support internal datasets
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryRecFields[numNestedSecondaryKeyFields + i] = primaryRecDesc.getFields()[i];
            secondaryTypeTraits[numNestedSecondaryKeyFields + i] = primaryRecDesc.getTypeTraits()[i];
            enforcedRecFields[i] = primaryRecDesc.getFields()[i];
            enforcedTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
        }

        enforcedRecFields[numPrimaryKeys] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
        enforcedRecDesc = new RecordDescriptor(enforcedRecFields, enforcedTypeTraits);
        if (numFilterFields > 0) {
            rtreeFields = new int[numNestedSecondaryKeyFields + numPrimaryKeys];
            for (int i = 0; i < rtreeFields.length; i++) {
                rtreeFields[i] = i;
            }

            Pair<IAType, Boolean> typePair = Index.getNonNullableKeyFieldType(filterFieldName, itemType);
            IAType type = typePair.first;
            ISerializerDeserializer serde = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(type);
            secondaryRecFields[numPrimaryKeys + numNestedSecondaryKeyFields] = serde;
        }
        secondaryRecDesc = new RecordDescriptor(secondaryRecFields);
        primaryKeyFields = new int[numPrimaryKeys];
        for (int i = 0; i < primaryKeyFields.length; i++) {
            primaryKeyFields[i] = i + numNestedSecondaryKeyFields;
        }
        if (isPointMBR) {
            int numNestedSecondaryKeyFieldForPointMBR = numNestedSecondaryKeyFields / 2;
            ISerializerDeserializer[] recFieldsForPointMBR = new ISerializerDeserializer[numPrimaryKeys
                    + numNestedSecondaryKeyFieldForPointMBR + numFilterFields];
            int idx = 0;
            for (int i = 0; i < numNestedSecondaryKeyFieldForPointMBR; i++) {
                recFieldsForPointMBR[idx++] = secondaryRecFields[i];
            }
            for (int i = 0; i < numPrimaryKeys + numFilterFields; i++) {
                recFieldsForPointMBR[idx++] = secondaryRecFields[numNestedSecondaryKeyFields + i];
            }
            secondaryRecDescForPointMBR = new RecordDescriptor(recFieldsForPointMBR);
        }
    }

    @Override
    public JobSpecification buildLoadingJobSpec() throws AlgebricksException {
        /***************************************************
         * [ About PointMBR Optimization ]
         * Instead of storing a MBR(4 doubles) for a point(2 doubles) in RTree leaf node,
         * PointMBR concept is introduced.
         * PointMBR is a way to store a point as 2 doubles in RTree leaf node.
         * This reduces RTree index size roughly in half.
         * In order to fully benefit from the PointMBR concept, besides RTree,
         * external sort operator during bulk-loading (from either data loading or index creation)
         * must deal with point as 2 doubles instead of 4 doubles. Otherwise, external sort will suffer from twice as
         * many doubles as it actually requires. For this purpose,
         * PointMBR specific optimization logic is added as follows:
         * 1) CreateMBR function in assign operator generates 2 doubles, instead of 4 doubles.
         * 2) External sort operator sorts points represented with 2 doubles.
         * 3) Bulk-loading in RTree takes 4 doubles by reading 2 doubles twice and then,
         * do the same work as non-point MBR cases.
         ***************************************************/
        JobSpecification spec = RuntimeUtils.createJobSpecification(metadataProvider.getApplicationContext());
        int numNestedSecondaryKeFieldsConsideringPointMBR =
                isPointMBR ? numNestedSecondaryKeyFields / 2 : numNestedSecondaryKeyFields;
        RecordDescriptor secondaryRecDescConsideringPointMBR = isPointMBR
                ? getTaggedRecordDescriptor(secondaryRecDescForPointMBR) : getTaggedRecordDescriptor(secondaryRecDesc);

        boolean isOverridingKeyFieldTypes = index.isOverridingKeyFieldTypes();

        assert dataset.getDatasetType() == DatasetType.INTERNAL;

        // Create dummy key provider for feeding the primary index scan.
        IOperatorDescriptor keyProviderOp = DatasetUtil.createDummyKeyProviderOp(spec, dataset, metadataProvider);
        IndexUtil.bindJobEventListener(spec, metadataProvider);

        // Create primary index scan op.
        IOperatorDescriptor primaryScanOp = createPrimaryIndexScanDiskComponentsOp(spec, metadataProvider,
                getTaggedRecordDescriptor(dataset.getPrimaryRecordDescriptor(metadataProvider)));

        // Assign op.
        IOperatorDescriptor sourceOp = primaryScanOp;
        if (isOverridingKeyFieldTypes && !enforcedItemType.equals(itemType)) {
            sourceOp = createCastOp(spec, dataset.getDatasetType(), index.isEnforced());
            spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
        }

        AlgebricksMetaOperatorDescriptor asterixAssignOp = createAssignOp(spec,
                numNestedSecondaryKeFieldsConsideringPointMBR, secondaryRecDescConsideringPointMBR);

        // Generate compensate tuples for upsert
        IOperatorDescriptor processorOp = createTupleProcessorOp(spec, secondaryRecDescConsideringPointMBR,
                numNestedSecondaryKeFieldsConsideringPointMBR, numPrimaryKeys, false);

        ExternalSortOperatorDescriptor sortOp = createSortOp(spec,
                getTaggedSecondaryComparatorFactories(new IBinaryComparatorFactory[] {
                        MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length) }),
                secondaryRecDescConsideringPointMBR);

        // Create secondary RTree bulk load op.
        LSMSecondaryIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp = createTreeIndexBulkLoadOp(spec,
                metadataProvider, secondaryRecDescConsideringPointMBR, createFieldPermutationForBulkLoadOp(),
                numNestedSecondaryKeFieldsConsideringPointMBR, numPrimaryKeys, false);

        SinkRuntimeFactory sinkRuntimeFactory = new SinkRuntimeFactory();
        sinkRuntimeFactory.setSourceLocation(sourceLoc);
        AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                new IPushRuntimeFactory[] { sinkRuntimeFactory }, new RecordDescriptor[] {});
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
    protected int[] createFieldPermutationForBulkLoadOp() {
        if (isPointMBR) {
            int[] fieldPermutation =
                    new int[NUM_TAG_FIELDS + numNestedSecondaryKeyFields + numPrimaryKeys + numFilterFields];
            int idx = 0;
            int numSecondaryKeyFieldsForPointMBR = numNestedSecondaryKeyFields / 2;
            for (int i = 0; i < NUM_TAG_FIELDS + numSecondaryKeyFieldsForPointMBR; i++) {
                fieldPermutation[idx++] = i;
            }
            //add the rest of the sk fields for pointMBR
            for (int i = 0; i < numSecondaryKeyFieldsForPointMBR; i++) {
                fieldPermutation[idx++] = NUM_TAG_FIELDS + i;
            }
            //add the pk and filter fields
            int end = numSecondaryKeyFieldsForPointMBR + numPrimaryKeys + numFilterFields;
            for (int i = numSecondaryKeyFieldsForPointMBR; i < end; i++) {
                fieldPermutation[idx++] = NUM_TAG_FIELDS + i;
            }
            return fieldPermutation;
        } else {
            return super.createFieldPermutationForBulkLoadOp();
        }

    }
}
