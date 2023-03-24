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
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.metadata.api.IResourceFactoryProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.metadata.entities.InternalDatasetDetails;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.algebricks.data.ITypeTraitProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManagerFactory;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallbackFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationSchedulerProvider;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTrackerFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMPageWriteCallbackFactory;
import org.apache.hyracks.storage.am.lsm.rtree.dataflow.LSMRTreeWithAntiMatterLocalResourceFactory;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.util.LogRedactionUtil;

public class RTreeResourceFactoryProvider implements IResourceFactoryProvider {

    private static final RTreePolicyType rTreePolicyType = RTreePolicyType.RTREE;
    public static final RTreeResourceFactoryProvider INSTANCE = new RTreeResourceFactoryProvider();

    private RTreeResourceFactoryProvider() {
    }

    @Override
    public IResourceFactory getResourceFactory(MetadataProvider mdProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories) throws AlgebricksException {
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        if (indexDetails.getKeyFieldNames().size() != 1) {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_INDEX_NUM_OF_FIELD,
                    indexDetails.getKeyFieldNames().size(), index.getIndexType(), 1);
        }
        IAType spatialType = Index.getNonNullableOpenFieldType(index, indexDetails.getKeyFieldTypes().get(0),
                indexDetails.getKeyFieldNames().get(0), recordType).first;
        if (spatialType == null) {
            throw new CompilationException(ErrorCode.COMPILATION_FIELD_NOT_FOUND,
                    LogRedactionUtil.userData(RecordUtil.toFullyQualifiedName(indexDetails.getKeyFieldNames().get(0))));
        }
        List<List<String>> primaryKeyFields = dataset.getPrimaryKeys();
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
        boolean isPointMBR = spatialType.getTypeTag() == ATypeTag.POINT || spatialType.getTypeTag() == ATypeTag.POINT3D;
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
            secondaryTypeTraits[numNestedSecondaryKeyFields + i] =
                    (dataset.getDatasetType() == DatasetType.INTERNAL) ? primaryTypeTraits[i] : null;
        }
        int[] rtreeFields = null;
        if (filterTypeTraits != null && filterTypeTraits.length > 0) {
            rtreeFields = new int[numNestedSecondaryKeyFields + numPrimaryKeys];
            for (int i = 0; i < rtreeFields.length; i++) {
                rtreeFields[i] = i;
            }
        }
        IStorageManager storageManager = storageComponentProvider.getStorageManager();
        ILSMOperationTrackerFactory opTrackerFactory = dataset.getIndexOperationTrackerFactory(index);
        ILSMIOOperationCallbackFactory ioOpCallbackFactory = dataset.getIoOperationCallbackFactory(index);
        ILSMPageWriteCallbackFactory pageWriteCallbackFactory = dataset.getPageWriteCallbackFactory();
        IMetadataPageManagerFactory metadataPageManagerFactory =
                storageComponentProvider.getMetadataPageManagerFactory();
        ILSMIOOperationSchedulerProvider ioSchedulerProvider =
                storageComponentProvider.getIoOperationSchedulerProvider();
        ILinearizeComparatorFactory linearizeCmpFactory =
                MetadataProvider.proposeLinearizer(keyType, secondaryComparatorFactories.length);
        ITypeTraits[] typeTraits = getTypeTraits(mdProvider, dataset, index, recordType, metaType);
        IBinaryComparatorFactory[] rtreeCmpFactories = getCmpFactories(mdProvider, index, recordType, metaType);
        int[] secondaryFilterFields = (filterTypeTraits != null && filterTypeTraits.length > 0)
                ? new int[] { numNestedSecondaryKeyFields + numPrimaryKeys } : null;
        IBinaryComparatorFactory[] btreeCompFactories = dataset.getDatasetType() == DatasetType.EXTERNAL ? null
                : getComparatorFactoriesForDeletedKeyBTree(secondaryTypeTraits, primaryComparatorFactories,
                        secondaryComparatorFactories);
        ITypeTraitProvider typeTraitProvider = mdProvider.getDataFormat().getTypeTraitProvider();
        if (dataset.getDatasetType() == DatasetType.INTERNAL) {
            AsterixVirtualBufferCacheProvider vbcProvider =
                    new AsterixVirtualBufferCacheProvider(dataset.getDatasetId());
            return new LSMRTreeWithAntiMatterLocalResourceFactory(storageManager, typeTraits, rtreeCmpFactories,
                    filterTypeTraits, filterCmpFactories, secondaryFilterFields, opTrackerFactory, ioOpCallbackFactory,
                    pageWriteCallbackFactory, metadataPageManagerFactory, vbcProvider, ioSchedulerProvider,
                    mergePolicyFactory, mergePolicyProperties, true, valueProviderFactories, rTreePolicyType,
                    linearizeCmpFactory, rtreeFields, isPointMBR, btreeCompFactories,
                    typeTraitProvider.getTypeTrait(BuiltinType.ANULL), NullIntrospector.INSTANCE);
        } else {
            return null;
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

    private static ITypeTraits[] getTypeTraits(MetadataProvider metadataProvider, Dataset dataset, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        ITypeTraitProvider ttProvider = metadataProvider.getStorageComponentProvider().getTypeTraitProvider();
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        List<List<String>> secondaryKeyFields = indexDetails.getKeyFieldNames();
        int numSecondaryKeys = secondaryKeyFields.size();
        int numPrimaryKeys = dataset.getPrimaryKeys().size();
        ITypeTraits[] primaryTypeTraits = dataset.getPrimaryTypeTraits(metadataProvider, recordType, metaType);
        if (numSecondaryKeys != 1) {
            throw new AsterixException("Cannot use " + numSecondaryKeys + " fields as a key for the R-tree index. "
                    + "There can be only one field as a key for the R-tree index.");
        }
        ARecordType sourceType;
        List<Integer> keySourceIndicators = indexDetails.getKeyFieldSourceIndicators();
        if (keySourceIndicators == null || keySourceIndicators.get(0) == 0) {
            sourceType = recordType;
        } else {
            sourceType = metaType;
        }
        Pair<IAType, Boolean> spatialTypePair = Index.getNonNullableOpenFieldType(index,
                indexDetails.getKeyFieldTypes().get(0), secondaryKeyFields.get(0), sourceType);
        IAType spatialType = spatialTypePair.first;
        if (spatialType == null) {
            throw new AsterixException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
        }
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numNestedSecondaryKeyFields = numDimensions * 2;
        ITypeTraits[] secondaryTypeTraits = new ITypeTraits[numNestedSecondaryKeyFields + numPrimaryKeys];
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
        for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
            secondaryTypeTraits[i] = ttProvider.getTypeTrait(nestedKeyType);
        }
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryTypeTraits[numNestedSecondaryKeyFields + i] = primaryTypeTraits[i];
        }
        return secondaryTypeTraits;
    }

    private static IBinaryComparatorFactory[] getCmpFactories(MetadataProvider metadataProvider, Index index,
            ARecordType recordType, ARecordType metaType) throws AlgebricksException {
        IBinaryComparatorFactoryProvider cmpFactoryProvider =
                metadataProvider.getStorageComponentProvider().getComparatorFactoryProvider();
        Index.ValueIndexDetails indexDetails = (Index.ValueIndexDetails) index.getIndexDetails();
        List<List<String>> secondaryKeyFields = indexDetails.getKeyFieldNames();
        int numSecondaryKeys = secondaryKeyFields.size();
        if (numSecondaryKeys != 1) {
            throw new AsterixException("Cannot use " + numSecondaryKeys + " fields as a key for the R-tree index. "
                    + "There can be only one field as a key for the R-tree index.");
        }
        List<Integer> keySourceIndicators = indexDetails.getKeyFieldSourceIndicators();
        ARecordType sourceType;
        if (keySourceIndicators == null || keySourceIndicators.get(0) == 0) {
            sourceType = recordType;
        } else {
            sourceType = metaType;
        }
        Pair<IAType, Boolean> spatialTypePair = Index.getNonNullableOpenFieldType(index,
                indexDetails.getKeyFieldTypes().get(0), secondaryKeyFields.get(0), sourceType);
        IAType spatialType = spatialTypePair.first;
        if (spatialType == null) {
            throw new AsterixException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
        }
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(spatialType.getTypeTag());
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numNestedSecondaryKeyFields = numDimensions * 2;
        IBinaryComparatorFactory[] secondaryComparatorFactories =
                new IBinaryComparatorFactory[numNestedSecondaryKeyFields];
        for (int i = 0; i < numNestedSecondaryKeyFields; i++) {
            secondaryComparatorFactories[i] = cmpFactoryProvider.getBinaryComparatorFactory(nestedKeyType, true);
        }
        return secondaryComparatorFactories;
    }
}
