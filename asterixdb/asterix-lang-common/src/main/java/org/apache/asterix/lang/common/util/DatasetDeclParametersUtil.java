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
package org.apache.asterix.lang.common.util;

import java.util.List;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.RecordTypeDefinition;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class DatasetDeclParametersUtil {
    /* ***********************************************
     * Merge Policy Parameters
     * ***********************************************
     */
    public static final String MERGE_POLICY_PARAMETER_NAME = "merge-policy";
    public static final String MERGE_POLICY_NAME_PARAMETER_NAME = "name";
    public static final String MERGE_POLICY_PARAMETERS_PARAMETER_NAME = "parameters";
    public static final String MERGE_POLICY_MERGABLE_SIZE_PARAMETER_NAME = "max-mergable-component-size";
    public static final String MERGE_POLICY_TOLERANCE_COUNT_PARAMETER_NAME = "max-tolerance-component-count";
    public static final String MERGE_POLICY_NUMBER_COMPONENTS_PARAMETER_NAME = "num-components";

    public static final String MERGE_POLICY_SIZE_RATIO_NAME = "size-ratio";
    public static final String MERGE_POLICY_MAX_COMPONENT_COUNT_NAME = "max-component-count";
    public static final String MERGE_POLICY_MIN_MERGE_COMPONENT_COUNT_NAME = "min-merge-component-count";
    public static final String MERGE_POLICY_MAX_MERGE_COMPONENT_COUNT_NAME = "max-merge-component-count";

    /* ***********************************************
     * Storage Block Compression Parameters
     * ***********************************************
     */
    public static final String STORAGE_BLOCK_COMPRESSION_PARAMETER_NAME = "storage-block-compression";
    public static final String STORAGE_BLOCK_COMPRESSION_SCHEME_PARAMETER_NAME = "scheme";

    /* ***********************************************
     * Node Group Parameters
     * ***********************************************
     */
    public static final String NODE_GROUP_NAME = "node-group";
    public static final String NODE_GROUP_NAME_PARAMETER_NAME = "name";

    /* ***********************************************
     * Dataset Format Type
     * ***********************************************
     */
    public static final String DATASET_FORMAT_PARAMETER_NAME = "storage-format";
    public static final String DATASET_FORMAT_FORMAT_PARAMETER_NAME = "format";
    public static final String DATASET_FORMAT_MAX_TUPLE_COUNT_PARAMETER_NAME = "max-tuple-count";
    public static final String DATASET_FORMAT_FREE_SPACE_TOLERANCE_PARAMETER_NAME = "free-space-tolerance";
    public static final String DATASET_FORMAT_FREE_MAX_LEAF_NODE_SIZE = "max-leaf-node-size";

    /* ***********************************************
     * Private members
     * ***********************************************
     */
    private static final ARecordType WITH_OBJECT_TYPE = getWithObjectType();
    static final AdmObjectNode EMPTY_WITH_OBJECT = new AdmObjectNode();

    private DatasetDeclParametersUtil() {
    }

    public static AdmObjectNode validateAndGetWithObjectNode(RecordConstructor withRecord,
            DatasetConfig.DatasetType datasetType) throws CompilationException {
        if (withRecord == null) {
            return EMPTY_WITH_OBJECT;
        }

        // Handle based on dataset type
        if (datasetType == DatasetConfig.DatasetType.INTERNAL) {
            final ConfigurationTypeValidator validator = new ConfigurationTypeValidator();
            final AdmObjectNode node = ExpressionUtils.toNode(withRecord);
            validator.validateType(WITH_OBJECT_TYPE, node);
            return node;
        } else {
            return ExpressionUtils.toNode(withRecord);
        }
    }

    private static ARecordType getWithObjectType() {
        final String[] withNames = { MERGE_POLICY_PARAMETER_NAME, STORAGE_BLOCK_COMPRESSION_PARAMETER_NAME,
                NODE_GROUP_NAME, DATASET_FORMAT_PARAMETER_NAME };
        final IAType[] withTypes = { AUnionType.createUnknownableType(getMergePolicyType()),
                AUnionType.createUnknownableType(getStorageBlockCompressionType()),
                AUnionType.createUnknownableType(getNodeGroupType()),
                AUnionType.createUnknownableType(getDatasetFormatType()) };
        return new ARecordType("withObject", withNames, withTypes, false);
    }

    private static ARecordType getMergePolicyType() {
        //merge-policy.parameters
        final String[] parameterNames = { MERGE_POLICY_MERGABLE_SIZE_PARAMETER_NAME,
                MERGE_POLICY_TOLERANCE_COUNT_PARAMETER_NAME, MERGE_POLICY_NUMBER_COMPONENTS_PARAMETER_NAME,
                MERGE_POLICY_SIZE_RATIO_NAME, MERGE_POLICY_MAX_COMPONENT_COUNT_NAME,
                MERGE_POLICY_MIN_MERGE_COMPONENT_COUNT_NAME, MERGE_POLICY_MAX_MERGE_COMPONENT_COUNT_NAME };
        final IAType[] parametersTypes = { AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.AINT64) };
        final ARecordType parameters =
                new ARecordType(MERGE_POLICY_PARAMETERS_PARAMETER_NAME, parameterNames, parametersTypes, false);

        //merge-policy
        final String[] mergePolicyNames = { MERGE_POLICY_NAME_PARAMETER_NAME, MERGE_POLICY_PARAMETERS_PARAMETER_NAME };
        final IAType[] mergePolicyTypes = { BuiltinType.ASTRING, AUnionType.createUnknownableType(parameters) };

        return new ARecordType(MERGE_POLICY_PARAMETER_NAME, mergePolicyNames, mergePolicyTypes, false);
    }

    private static ARecordType getStorageBlockCompressionType() {
        final String[] schemeName = { STORAGE_BLOCK_COMPRESSION_SCHEME_PARAMETER_NAME };
        final IAType[] schemeType = { BuiltinType.ASTRING };
        return new ARecordType(STORAGE_BLOCK_COMPRESSION_PARAMETER_NAME, schemeName, schemeType, false);
    }

    private static ARecordType getNodeGroupType() {
        final String[] schemeName = { NODE_GROUP_NAME_PARAMETER_NAME };
        final IAType[] schemeType = { BuiltinType.ASTRING };
        return new ARecordType(NODE_GROUP_NAME, schemeName, schemeType, false);
    }

    /**
     * Adjusts dataset inline type definition if it has primary key specification:
     * forces NOT UNKNOWN on fields that are part of primary key.
     */
    public static void adjustInlineTypeDecl(TypeExpression typeDecl, List<List<String>> primaryKeyFields,
            List<Integer> primaryKeySources, boolean isMeta) {
        switch (typeDecl.getTypeKind()) {
            case RECORD:
                RecordTypeDefinition recordTypeDef = (RecordTypeDefinition) typeDecl;
                for (int i = 0, n = primaryKeyFields.size(); i < n; i++) {
                    List<String> primaryKeyPath = primaryKeyFields.get(i);
                    if (primaryKeyPath.size() == 1) {
                        String primaryKeyFieldName = primaryKeyPath.get(0);
                        boolean isMetaSource =
                                primaryKeySources != null && primaryKeySources.get(i) == Index.META_RECORD_INDICATOR;
                        boolean isSameSource = isMetaSource == isMeta;
                        if (isSameSource) {
                            int fieldIdx = recordTypeDef.getFieldNames().indexOf(primaryKeyFieldName);
                            if (fieldIdx >= 0) {
                                recordTypeDef.getMissableFields().set(fieldIdx, false);
                                recordTypeDef.getNullableFields().set(fieldIdx, false);
                            }
                        }
                    }
                }
                break;
            case TYPEREFERENCE:
                // this is not an inline type decl
                break;
            default:
                throw new IllegalStateException(typeDecl.getTypeKind().toString());
        }
    }

    private static ARecordType getDatasetFormatType() {
        final String[] formatFieldNames =
                { DATASET_FORMAT_FORMAT_PARAMETER_NAME, DATASET_FORMAT_MAX_TUPLE_COUNT_PARAMETER_NAME,
                        DATASET_FORMAT_FREE_SPACE_TOLERANCE_PARAMETER_NAME, DATASET_FORMAT_FREE_MAX_LEAF_NODE_SIZE };
        final IAType[] formatFieldTypes = { BuiltinType.ASTRING, AUnionType.createUnknownableType(BuiltinType.AINT64),
                AUnionType.createUnknownableType(BuiltinType.ADOUBLE),
                AUnionType.createUnknownableType(BuiltinType.ASTRING) };
        return new ARecordType(DATASET_FORMAT_PARAMETER_NAME, formatFieldNames, formatFieldTypes, false);
    }
}
