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
import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.context.AsterixVirtualBufferCacheProvider;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.indexing.IndexingConstants;
import org.apache.asterix.metadata.api.IIndexDataflowHelperFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeWithAntiMatterTuplesDataflowHelperFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class RTreeDataflowHelperFactoryProvider implements IIndexDataflowHelperFactoryProvider {

    public static final RTreeDataflowHelperFactoryProvider INSTANCE = new RTreeDataflowHelperFactoryProvider();

    private RTreeDataflowHelperFactoryProvider() {
    }

    protected RTreePolicyType rTreePolicyType() {
        return RTreePolicyType.RTREE;
    }

    @Override
    public IIndexDataflowHelperFactory getIndexDataflowHelperFactory(MetadataProvider mdProvider, Dataset dataset,
            Index index, ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        if (index.getKeyFieldNames().size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD,
                    index.getKeyFieldNames().size(), index.getIndexType(), 1);
        }
        IAType spatialType = Index.getNonNullableOpenFieldType(index.getKeyFieldTypes().get(0),
                index.getKeyFieldNames().get(0), recordType).first;
        if (spatialType == null) {
            throw new CompilationException(ErrorCode.COMPILATION_FIELD_NOT_FOUND,
                    StringUtils.join(index.getKeyFieldNames().get(0), '.'));
        }
        List<List<String>> primaryKeyFields = DatasetUtil.getPartitioningKeys(dataset);
        int numPrimaryKeys = primaryKeyFields.size();
        ITypeTraits[] primaryTypeTraits = null;
        IBinaryComparatorFactory[] primaryComparatorFactories = null;
        IStorageComponentProvider storageComponentProvider = mdProvider.getStorageComponentProvider();
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            primaryTypeTraits = new ITypeTraits[numPrimaryKeys + 1 + (dataset.hasMetaPart() ? 1 : 0)];
            primaryComparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
            List<Integer> indicators = null;
            if (dataset.hasMetaPart()) {
                indicators = ((InternalDatasetDetails) dataset.getDatasetDetails()).getKeySourceIndicator();
            }
            for (int i = 0; i < numPrimaryKeys; i++) {
                IAType keyType = (indicators == null || indicators.get(i) == 0)
                        ? recordType.getSubFieldType(primaryKeyFields.get(i))
                        : metaType.getSubFieldType(primaryKeyFields.get(i));
                primaryComparatorFactories[i] = storageComponentProvider.getComparatorFactoryProvider()
                        .getBinaryComparatorFactory(keyType, true);
                primaryTypeTraits[i] = storageComponentProvider.getTypeTraitProvider().getTypeTrait(keyType);
            }
            primaryTypeTraits[numPrimaryKeys] =
                    storageComponentProvider.getTypeTraitProvider().getTypeTrait(recordType);
            if (dataset.hasMetaPart()) {
                primaryTypeTraits[numPrimaryKeys + 1] =
                        storageComponentProvider.getTypeTraitProvider().getTypeTrait(recordType);
            }
        }
        boolean isPointMBR =
                spatialType.getTypeTag() == ATypeTag.POINT || spatialType.getTypeTag() == ATypeTag.POINT3D;
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numNestedSecondaryKeyFields = numDimensions * 2;
        IBinaryComparatorFactory[] secondaryComparatorFactories =
                new IBinaryComparatorFactory[numNestedSecondaryKeyFields];
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                new IPrimitiveValueProviderFactory[numNestedSecondaryKeyFields];
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numNestedSecondaryKeyFields + numPrimaryKeys];
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
        ATypeTag keyType = nestedKeyType.getTypeTag();
        for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
            secondaryComparatorFactories[i] = storageComponentProvider.getComparatorFactoryProvider()
                    .getBinaryComparatorFactory(nestedKeyType, true);
            secondaryTypeTraits[i] = storageComponentProvider.getTypeTraitProvider().getTypeTrait(nestedKeyType);
            valueProviderFactories[i] = storageComponentProvider.getPrimitiveValueProviderFactory();

        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryTypeTraits[numNestedSecondaryKeyFields + i] = (dataset.getDatasetType() == DatasetType.INTERNAL)
                    ? primaryTypeTraits[i] : IndexingConstants.getTypeTraits(i);
        }
        int[] rtreeFields = null;
        if (filterTypeTraits != null && filterTypeTraits.length > 0) {
            rtreeFields = new int[numNestedSecondaryKeyFields + numPrimaryKeys];
            for (int i = 0; i < rtreeFields.length; i++) {
                rtreeFields[i] = i;
            }
        }
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            int[] secondaryFilterFields = (filterTypeTraits != null && filterTypeTraits.length > 0)
                    ? new int[] { numNestedSecondaryKeyFields + numPrimaryKeys } : null;
            IBinaryComparatorFactory[] btreeCompFactories = getComparatorFactoriesForDeletedKeyBTree(
                    secondaryTypeTraits, primaryComparatorFactories, secondaryComparatorFactories);
            return new LSMRTreeWithAntiMatterTuplesDataflowHelperFactory(valueProviderFactories, rTreePolicyType(),
                    btreeCompFactories, new AsterixVirtualBufferCacheProvider(dataset.getDatasetId()),
                    mergePolicyFactory, mergePolicyProperties, dataset.getIndexOperationTrackerFactory(index),
                    storageComponentProvider.getIoOperationSchedulerProvider(),
                    dataset.getIoOperationCallbackFactory(index),
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length), rtreeFields,
                    filterTypeTraits, filterCmpFactories, secondaryFilterFields, !dataset.getDatasetDetails().isTemp(),
                    isPointMBR);
        } else {
            return new ExternalRTreeDataflowHelperFactory(valueProviderFactories, rTreePolicyType(),
                    ExternalIndexingOperations.getBuddyBtreeComparatorFactories(), mergePolicyFactory,
                    mergePolicyProperties, dataset.getIndexOperationTrackerFactory(index),
                    storageComponentProvider.getIoOperationSchedulerProvider(),
                    dataset.getIoOperationCallbackFactory(index),
                    MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length),
                    mdProvider.getStorageProperties().getBloomFilterFalsePositiveRate(),
                    new int[] { numNestedSecondaryKeyFields },
                    ExternalDatasetsRegistry.INSTANCE.getDatasetVersion(dataset), true, isPointMBR);
        }
    }

    private static IBinaryComparatorFactory[] getComparatorFactoriesForDeletedKeyBTree(
            ITypeTraits[] secondaryTypeTraits, IBinaryComparatorFactory[] primaryComparatorFactories,
            IBinaryComparatorFactory[] secondaryComparatorFactories) {
        IBinaryComparatorFactory[] btreeCompFactories = new IBinaryComparatorFactory[secondaryTypeTraits.length];
        int i = 0;
        for (; i < secondaryComparatorFactories.length; i++) {
            btreeCompFactories[i] = secondaryComparatorFactories[i];
        }
        for (int j = 0; i < secondaryTypeTraits.length; i++, j++) {
            btreeCompFactories[i] = primaryComparatorFactories[j];
        }
        return btreeCompFactories;
    }
}