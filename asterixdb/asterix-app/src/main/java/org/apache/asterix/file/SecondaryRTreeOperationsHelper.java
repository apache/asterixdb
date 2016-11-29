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
package org.apache.asterix.file;

import java.util.List;

import org.apache.asterix.app.external.ExternalIndexingOperations;
import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.GlobalConfig;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallbackFactory;
import org.apache.asterix.common.transactions.IResourceFactory;
import org.apache.asterix.dataflow.data.nontagged.valueproviders.AqlPrimitiveValueProviderFactory;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.external.operators.ExternalDataScanOperatorDescriptor;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.utils.ExternalDatasetsRegistry;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.asterix.runtime.util.AsterixRuntimeComponentsProvider;
import org.apache.asterix.transaction.management.opcallbacks.SecondaryIndexOperationTrackerProvider;
import org.apache.asterix.transaction.management.resource.ExternalRTreeLocalResourceMetadataFactory;
import org.apache.asterix.transaction.management.resource.LSMRTreeLocalResourceMetadataFactory;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceFactoryProvider;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.jobgen.impl.ConnectorPolicyAssignmentPolicy;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.base.SinkRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import org.apache.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexBulkLoadOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexCreateOperatorDescriptor;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexCompactOperatorDescriptor;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeWithAntiMatterTuplesDataflowHelperFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;
import org.apache.hyracks.storage.common.file.LocalResource;

@SuppressWarnings("rawtypes")
public class SecondaryRTreeOperationsHelper extends SecondaryIndexOperationsHelper {

    protected IPrimitiveValueProviderFactory[] valueProviderFactories;
    protected int numNestedSecondaryKeyFields;
    protected ATypeTag keyType;
    protected int[] primaryKeyFields;
    protected int[] rtreeFields;
    protected boolean isPointMBR;
    protected RecordDescriptor secondaryRecDescForPointMBR = null;

    protected SecondaryRTreeOperationsHelper(PhysicalOptimizationConfig physOptConf,
            IAsterixPropertiesProvider propertiesProvider) {
        super(physOptConf, propertiesProvider);
    }

    @Override
    public JobSpecification buildCreationJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
        boolean temp = dataset.getDatasetDetails().isTemp();
        IIndexDataflowHelperFactory indexDataflowHelperFactory;
        ILocalResourceFactoryProvider localResourceFactoryProvider;
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {

            IBinaryComparatorFactory[] btreeCompFactories = getComparatorFactoriesForDeletedKeyBTree();

            //prepare a LocalResourceMetadata which will be stored in NC's local resource repository
            IResourceFactory localResourceMetadata = new LSMRTreeLocalResourceMetadataFactory(secondaryTypeTraits,
                    secondaryComparatorFactories, btreeCompFactories, valueProviderFactories, RTreePolicyType.RTREE,
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length),
                    dataset.getDatasetId(), mergePolicyFactory, mergePolicyFactoryProperties, filterTypeTraits,
                    filterCmpFactories, rtreeFields, primaryKeyFields, secondaryFilterFields, isPointMBR);
            localResourceFactoryProvider =
                    new PersistentLocalResourceFactoryProvider(localResourceMetadata, LocalResource.LSMRTreeResource);
            indexDataflowHelperFactory = new LSMRTreeWithAntiMatterTuplesDataflowHelperFactory(valueProviderFactories,
                    RTreePolicyType.RTREE, btreeCompFactories,
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), mergePolicyFactory,
                    mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length), rtreeFields,
                    filterTypeTraits, filterCmpFactories, secondaryFilterFields, !temp, isPointMBR);
        } else {
            // External dataset
            // Prepare a LocalResourceMetadata which will be stored in NC's local resource repository
            IResourceFactory localResourceMetadata = new ExternalRTreeLocalResourceMetadataFactory(secondaryTypeTraits,
                    secondaryComparatorFactories, ExternalIndexingOperations.getBuddyBtreeComparatorFactories(),
                    valueProviderFactories, RTreePolicyType.RTREE,
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length),
                    dataset.getDatasetId(), mergePolicyFactory, mergePolicyFactoryProperties, primaryKeyFields,
                    isPointMBR);
            localResourceFactoryProvider = new PersistentLocalResourceFactoryProvider(localResourceMetadata,
                    LocalResource.ExternalRTreeResource);

            indexDataflowHelperFactory = new ExternalRTreeDataflowHelperFactory(valueProviderFactories,
                    RTreePolicyType.RTREE, ExternalIndexingOperations.getBuddyBtreeComparatorFactories(),
                    mergePolicyFactory, mergePolicyFactoryProperties,
                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length),
                    storageProperties.getBloomFilterFalsePositiveRate(), new int[] { numNestedSecondaryKeyFields },
                    ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true, isPointMBR);
        }

        TreeIndexCreateOperatorDescriptor secondaryIndexCreateOp = new TreeIndexCreateOperatorDescriptor(spec,
                AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                secondaryFileSplitProvider, secondaryTypeTraits, secondaryComparatorFactories, null,
                indexDataflowHelperFactory, localResourceFactoryProvider, NoOpOperationCallbackFactory.INSTANCE);

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, secondaryIndexCreateOp,
                secondaryPartitionConstraint);
        spec.addRoot(secondaryIndexCreateOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }

    private IBinaryComparatorFactory[] getComparatorFactoriesForDeletedKeyBTree() {
        IBinaryComparatorFactory[] btreeCompFactories = new IBinaryComparatorFactory[secondaryTypeTraits.length];;
        int i = 0;
        for (; i < secondaryComparatorFactories.length; i++) {
            btreeCompFactories[i] = secondaryComparatorFactories[i];
        }
        for (int j = 0; i < secondaryTypeTraits.length; i++, j++) {
            btreeCompFactories[i] = primaryComparatorFactories[j];
        }
        return btreeCompFactories;
    }

    @Override
    protected int getNumSecondaryKeys() {
        return numNestedSecondaryKeyFields;
    }

    @Override
    protected void setSecondaryRecDescAndComparators(IndexType indexType, List<List<String>> secondaryKeyFields,
            List<IAType> secondaryKeyTypes, int gramLength, MetadataProvider metadata)
            throws AlgebricksException, AsterixException {
        int numSecondaryKeys = secondaryKeyFields.size();
        if (numSecondaryKeys != 1) {
            throw new AsterixException("Cannot use " + numSecondaryKeys
                    + " fields as a key for the R-tree index. There can be only one field as a key for the R-tree index.");
        }
        Pair<IAType, Boolean> spatialTypePair =
                Index.getNonNullableOpenFieldType(secondaryKeyTypes.get(0), secondaryKeyFields.get(0), itemType);
        IAType spatialType = spatialTypePair.first;
        anySecondaryKeyIsNullable = spatialTypePair.second;
        if (spatialType == null) {
            throw new AsterixException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
        }
        isPointMBR = spatialType.getTypeTag() == ATypeTag.POINT || spatialType.getTypeTag() == ATypeTag.POINT3D;
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        numNestedSecondaryKeyFields = numDimensions * 2;
        int recordColumn = dataset.getDatasetType() == DatasetType.INTERNAL ? numPrimaryKeys : 0;
        secondaryFieldAccessEvalFactories =
                metadata.getFormat().createMBRFactory(isEnforcingKeyTypes ? enforcedItemType : itemType,
                        secondaryKeyFields.get(0), recordColumn, numDimensions, filterFieldName);
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
            valueProviderFactories[i] = AqlPrimitiveValueProviderFactory.INSTANCE;

        }
        // Add serializers and comparators for primary index fields.
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFields[numNestedSecondaryKeyFields + i] = primaryRecDesc.getFields()[i];
                secondaryTypeTraits[numNestedSecondaryKeyFields + i] = primaryRecDesc.getTypeTraits()[i];
                enforcedRecFields[i] = primaryRecDesc.getFields()[i];
                enforcedTypeTraits[i] = primaryRecDesc.getTypeTraits()[i];
            }
        } else {
            for (int i = 0; i < numPrimaryKeys; i++) {
                secondaryRecFields[numNestedSecondaryKeyFields + i] = IndexingConstants.getSerializerDeserializer(i);
                secondaryTypeTraits[numNestedSecondaryKeyFields + i] = IndexingConstants.getTypeTraits(i);
                enforcedRecFields[i] = IndexingConstants.getSerializerDeserializer(i);
                enforcedTypeTraits[i] = IndexingConstants.getTypeTraits(i);
            }
        }
        enforcedRecFields[numPrimaryKeys] =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(itemType);
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
    public JobSpecification buildLoadingJobSpec() throws AsterixException, AlgebricksException {
        /***************************************************
         * [ About PointMBR Optimization ]
         * Instead of storing a MBR(4 doubles) for a point(2 doubles) in RTree leaf node, PointMBR concept is introduced.
         * PointMBR is a way to store a point as 2 doubles in RTree leaf node.
         * This reduces RTree index size roughly in half.
         * In order to fully benefit from the PointMBR concept, besides RTree,
         * external sort operator during bulk-loading (from either data loading or index creation)
         * must deal with point as 2 doubles instead of 4 doubles. Otherwise, external sort will suffer from twice as
         * many doubles as it actually requires. For this purpose, PointMBR specific optimization logic is added as follows:
         * 1) CreateMBR function in assign operator generates 2 doubles, instead of 4 doubles.
         * 2) External sort operator sorts points represented with 2 doubles.
         * 3) Bulk-loading in RTree takes 4 doubles by reading 2 doubles twice and then, do the same work as non-point MBR cases.
         ***************************************************/
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        boolean temp = dataset.getDatasetDetails().isTemp();
        int[] fieldPermutation = createFieldPermutationForBulkLoadOp(numNestedSecondaryKeyFields);
        int numNestedSecondaryKeFieldsConsideringPointMBR =
                isPointMBR ? numNestedSecondaryKeyFields / 2 : numNestedSecondaryKeyFields;
        RecordDescriptor secondaryRecDescConsideringPointMBR =
                isPointMBR ? secondaryRecDescForPointMBR : secondaryRecDesc;
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            // Create dummy key provider for feeding the primary index scan.
            AbstractOperatorDescriptor keyProviderOp = createDummyKeyProviderOp(spec);

            // Create primary index scan op.
            BTreeSearchOperatorDescriptor primaryScanOp = createPrimaryIndexScanOp(spec);

            // Assign op.
            AbstractOperatorDescriptor sourceOp = primaryScanOp;
            if (isEnforcingKeyTypes && !enforcedItemType.equals(itemType)) {
                sourceOp = createCastOp(spec, dataset.getDatasetType());
                spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
            }
            AlgebricksMetaOperatorDescriptor asterixAssignOp = createAssignOp(spec, sourceOp,
                    numNestedSecondaryKeFieldsConsideringPointMBR, secondaryRecDescConsideringPointMBR);

            // If any of the secondary fields are nullable, then add a select op that filters nulls.
            AlgebricksMetaOperatorDescriptor selectOp = null;
            if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
                selectOp = createFilterNullsSelectOp(spec, numNestedSecondaryKeFieldsConsideringPointMBR,
                        secondaryRecDescConsideringPointMBR);
            }

            // Sort by secondary keys.
            ExternalSortOperatorDescriptor sortOp =
                    createSortOp(spec,
                            new IBinaryComparatorFactory[] { MetadataProvider.proposeLinearizer(keyType,
                                    secondaryComparatorFactories.length) },
                            isPointMBR ? secondaryRecDescForPointMBR : secondaryRecDesc);

            AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();

            IBinaryComparatorFactory[] btreeCompFactories = getComparatorFactoriesForDeletedKeyBTree();
            IIndexDataflowHelperFactory idff = new LSMRTreeWithAntiMatterTuplesDataflowHelperFactory(
                    valueProviderFactories, RTreePolicyType.RTREE, btreeCompFactories,
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), mergePolicyFactory,
                    mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length), rtreeFields,
                    filterTypeTraits, filterCmpFactories, secondaryFilterFields, !temp, isPointMBR);;

            // Create secondary RTree bulk load op.
            TreeIndexBulkLoadOperatorDescriptor secondaryBulkLoadOp =
                    createTreeIndexBulkLoadOp(spec, fieldPermutation, idff, GlobalConfig.DEFAULT_TREE_FILL_FACTOR);
            AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                    new IPushRuntimeFactory[] { new SinkRuntimeFactory() }, new RecordDescriptor[] {});
            // Connect the operators.
            spec.connect(new OneToOneConnectorDescriptor(spec), keyProviderOp, 0, primaryScanOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
            if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, sortOp, 0);
            } else {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
            }
            spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
            spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoadOp, 0, metaOp, 0);
            spec.addRoot(metaOp);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        } else {
            // External dataset
            /*
             * In case of external data, this method is used to build loading jobs for both initial load on index creation
             * and transaction load on dataset referesh
             */
            // Create external indexing scan operator
            ExternalDataScanOperatorDescriptor primaryScanOp = createExternalIndexingOp(spec);
            AbstractOperatorDescriptor sourceOp = primaryScanOp;
            if (isEnforcingKeyTypes && !enforcedItemType.equals(itemType)) {
                sourceOp = createCastOp(spec, dataset.getDatasetType());
                spec.connect(new OneToOneConnectorDescriptor(spec), primaryScanOp, 0, sourceOp, 0);
            }
            // Assign op.
            AlgebricksMetaOperatorDescriptor asterixAssignOp = createExternalAssignOp(spec,
                    numNestedSecondaryKeFieldsConsideringPointMBR, secondaryRecDescConsideringPointMBR);

            // If any of the secondary fields are nullable, then add a select op that filters nulls.
            AlgebricksMetaOperatorDescriptor selectOp = null;
            if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
                selectOp = createFilterNullsSelectOp(spec, numNestedSecondaryKeFieldsConsideringPointMBR,
                        secondaryRecDescConsideringPointMBR);
            }

            // Sort by secondary keys.
            ExternalSortOperatorDescriptor sortOp =
                    createSortOp(spec,
                            new IBinaryComparatorFactory[] { MetadataProvider.proposeLinearizer(keyType,
                                    secondaryComparatorFactories.length) },
                            isPointMBR ? secondaryRecDescForPointMBR : secondaryRecDesc);
            AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();

            // Create the dataflow helper factory
            ExternalRTreeDataflowHelperFactory dataflowHelperFactory = new ExternalRTreeDataflowHelperFactory(
                    valueProviderFactories, RTreePolicyType.RTREE, primaryComparatorFactories, mergePolicyFactory,
                    mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length),
                    storageProperties.getBloomFilterFalsePositiveRate(), new int[] { numNestedSecondaryKeyFields },
                    ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true, isPointMBR);
            // Create secondary RTree bulk load op.
            IOperatorDescriptor root;
            AbstractTreeIndexOperatorDescriptor secondaryBulkLoadOp;
            if (externalFiles != null) {
                // Transaction load
                secondaryBulkLoadOp = createExternalIndexBulkModifyOp(spec, fieldPermutation, dataflowHelperFactory,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR);
                root = secondaryBulkLoadOp;
            } else {
                // Initial load
                secondaryBulkLoadOp = createTreeIndexBulkLoadOp(spec, fieldPermutation, dataflowHelperFactory,
                        GlobalConfig.DEFAULT_TREE_FILL_FACTOR);
                AlgebricksMetaOperatorDescriptor metaOp = new AlgebricksMetaOperatorDescriptor(spec, 1, 0,
                        new IPushRuntimeFactory[] { new SinkRuntimeFactory() },
                        new RecordDescriptor[] { secondaryRecDesc });
                spec.connect(new OneToOneConnectorDescriptor(spec), secondaryBulkLoadOp, 0, metaOp, 0);
                root = metaOp;
            }

            spec.connect(new OneToOneConnectorDescriptor(spec), sourceOp, 0, asterixAssignOp, 0);
            if (anySecondaryKeyIsNullable || isEnforcingKeyTypes) {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, selectOp, 0);
                spec.connect(new OneToOneConnectorDescriptor(spec), selectOp, 0, sortOp, 0);
            } else {
                spec.connect(new OneToOneConnectorDescriptor(spec), asterixAssignOp, 0, sortOp, 0);
            }
            spec.connect(new OneToOneConnectorDescriptor(spec), sortOp, 0, secondaryBulkLoadOp, 0);
            spec.addRoot(root);
            spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        }
        return spec;
    }

    protected int[] createFieldPermutationForBulkLoadOp(int numSecondaryKeyFields) {
        int[] fieldPermutation = new int[numSecondaryKeyFields + numPrimaryKeys + numFilterFields];
        int numSecondaryKeyFieldsForPointMBR = numSecondaryKeyFields / 2;
        int end = isPointMBR ? numSecondaryKeyFieldsForPointMBR : fieldPermutation.length;
        for (int i = 0; i < end; i++) {
            fieldPermutation[i] = i;
        }
        if (isPointMBR) {
            /*******************************************************************************
             * For example, suppose that 2d point type data is indexed using RTree, there is no
             * filter fields, and a primary key consists of a single field.
             * ========== Without PointMBR optimization ==========
             * If there is no point type optimization, the input operator of RTree's TreeIndexBulkLoadOperator
             * delivers five variables to the TreeIndexBulkLoadOperator as follows:
             * [$var1, $var2, $var3, $var4, $var5]
             * where $var1 ~ $var4 together represent an MBR of a point object.
             * Since it is a point object, $var1 and $var3 have always identical values. So do $var2 and $var3.
             * $var5 represents a primary key value.
             * fieldPermutation variable captures this order correctly by putting values in the array as follows:
             * [0,1,2,3,4]
             * =========== With PointMBR optimization ===========
             * With PointMBR optimization, the input operator of RTree's TreeIndexBulkLoadOperator
             * delivers 3 variables to the TreeIndexBulkLoadOperator as follows:
             * [$var1, $var2, $var3]
             * where $var1 and $var2 together represent an MBR of a point object.
             * $var3 represents a primary key value.
             * fieldPermutation variable captures this order correctly by putting values in the array as follows:
             * [0,1,0,1,2]
             * This means that bulkloadOp reads the pair of $var1 and $var2 twice in order to provide the same
             * output just like when there were no PointMBR optimization available.
             * This adjustment is done in this if clause code.
             *********************************************************************************/
            int idx = numSecondaryKeyFieldsForPointMBR;
            //add the rest of the sk fields for pointMBR
            for (int i = 0; i < numSecondaryKeyFieldsForPointMBR; i++) {
                fieldPermutation[idx++] = i;
            }
            //add the pk and filter fields
            end = numSecondaryKeyFieldsForPointMBR + numPrimaryKeys + numFilterFields;
            for (int i = numSecondaryKeyFieldsForPointMBR; i < end; i++) {
                fieldPermutation[idx++] = i;
            }
        }
        return fieldPermutation;
    }

    @Override
    public JobSpecification buildCompactJobSpec() throws AsterixException, AlgebricksException {
        JobSpecification spec = JobSpecificationUtils.createJobSpecification();

        AsterixStorageProperties storageProperties = propertiesProvider.getStorageProperties();
        boolean temp = dataset.getDatasetDetails().isTemp();
        LSMTreeIndexCompactOperatorDescriptor compactOp;
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            IBinaryComparatorFactory[] btreeCompFactories = getComparatorFactoriesForDeletedKeyBTree();;
            IIndexDataflowHelperFactory idff = new LSMRTreeWithAntiMatterTuplesDataflowHelperFactory(
                    valueProviderFactories, RTreePolicyType.RTREE, btreeCompFactories,
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()), mergePolicyFactory,
                    mergePolicyFactoryProperties, new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, LSMRTreeIOOperationCallbackFactory.INSTANCE,
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length), rtreeFields,
                    filterTypeTraits, filterCmpFactories, secondaryFilterFields, !temp, isPointMBR);
            compactOp =
                    new LSMTreeIndexCompactOperatorDescriptor(spec, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider,
                            secondaryTypeTraits, secondaryComparatorFactories, secondaryBloomFilterKeyFields, idff,
                            NoOpOperationCallbackFactory.INSTANCE);
        } else {
            // External dataset
            compactOp =
                    new LSMTreeIndexCompactOperatorDescriptor(spec, AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                            AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER, secondaryFileSplitProvider,
                            secondaryTypeTraits, secondaryComparatorFactories, secondaryBloomFilterKeyFields,
                            new ExternalRTreeDataflowHelperFactory(valueProviderFactories, RTreePolicyType.RTREE,
                                    primaryComparatorFactories, mergePolicyFactory, mergePolicyFactoryProperties,
                                    new SecondaryIndexOperationTrackerProvider(dataset.getDatasetId()),
                                    AsterixRuntimeComponentsProvider.RUNTIME_PROVIDER,
                                    LSMRTreeIOOperationCallbackFactory.INSTANCE,
                                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length),
                                    storageProperties.getBloomFilterFalsePositiveRate(),
                                    new int[] { numNestedSecondaryKeyFields },
                                    ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true, isPointMBR),
                            NoOpOperationCallbackFactory.INSTANCE);
        }

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, compactOp,
                secondaryPartitionConstraint);
        spec.addRoot(compactOp);
        spec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        return spec;
    }
}
