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
package org.apache.asterix.lang.common.statement;

import java.util.Map;

import org.apache.asterix.common.config.DatasetConfig;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.ConfigurationUtil;
import org.apache.asterix.lang.common.util.DatasetDeclParametersUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.dataset.DatasetFormatInfo;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.runtime.compression.CompressionManager;
import org.apache.hyracks.util.StorageUtil;

public class DatasetDecl extends AbstractStatement {

    protected final Identifier name;
    protected final Namespace namespace;
    protected final TypeExpression itemType;
    protected final TypeExpression metaItemType;
    protected final DatasetType datasetType;
    protected final IDatasetDetailsDecl datasetDetailsDecl;
    protected final Map<String, String> hints;
    private final AdmObjectNode withObjectNode;
    protected final boolean ifNotExists;
    protected final Query query;

    public DatasetDecl(Namespace namespace, Identifier name, TypeExpression itemType, TypeExpression metaItemType,
            Map<String, String> hints, DatasetType datasetType, IDatasetDetailsDecl idd, RecordConstructor withRecord,
            boolean ifNotExists) throws CompilationException {
        this(namespace, name, itemType, metaItemType, hints, datasetType, idd, withRecord, ifNotExists, null);
    }

    public DatasetDecl(Namespace namespace, Identifier name, TypeExpression itemType, TypeExpression metaItemType,
            Map<String, String> hints, DatasetType datasetType, IDatasetDetailsDecl idd, RecordConstructor withRecord,
            boolean ifNotExists, Query query) throws CompilationException {
        this.namespace = namespace;
        this.name = name;
        this.itemType = itemType;
        this.metaItemType = metaItemType;
        this.hints = hints;
        this.withObjectNode = DatasetDeclParametersUtil.validateAndGetWithObjectNode(withRecord, datasetType);
        this.ifNotExists = ifNotExists;
        this.datasetType = datasetType;
        this.datasetDetailsDecl = idd;
        this.query = query;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    public DatasetType getDatasetType() {
        return datasetType;
    }

    public Identifier getName() {
        return name;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverse() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public TypeExpression getItemType() {
        return itemType;
    }

    public TypeExpression getMetaItemType() {
        return metaItemType;
    }

    public Query getQuery() {
        return query;
    }

    public String getNodegroupName() {
        AdmObjectNode nodeGroupObj = (AdmObjectNode) withObjectNode.get(DatasetDeclParametersUtil.NODE_GROUP_NAME);
        if (nodeGroupObj == null) {
            return null;
        }
        return nodeGroupObj.getOptionalString(DatasetDeclParametersUtil.NODE_GROUP_NAME_PARAMETER_NAME);
    }

    private AdmObjectNode getMergePolicyObject() {
        return (AdmObjectNode) withObjectNode.get(DatasetDeclParametersUtil.MERGE_POLICY_PARAMETER_NAME);
    }

    public String getCompactionPolicy() {
        AdmObjectNode mergePolicy = getMergePolicyObject();
        if (mergePolicy == null) {
            return null;
        }

        return mergePolicy.getOptionalString(DatasetDeclParametersUtil.MERGE_POLICY_NAME_PARAMETER_NAME);
    }

    public Map<String, String> getCompactionPolicyProperties() throws CompilationException {
        AdmObjectNode mergePolicy = getMergePolicyObject();
        if (mergePolicy == null) {
            return null;
        }
        IAdmNode mergePolicyParameters =
                mergePolicy.get(DatasetDeclParametersUtil.MERGE_POLICY_PARAMETERS_PARAMETER_NAME);
        if (mergePolicyParameters == null) {
            return null;
        }
        return ConfigurationUtil.toProperties((AdmObjectNode) mergePolicyParameters);
    }

    public String getDatasetCompressionScheme() {
        if (datasetType != DatasetType.INTERNAL) {
            return CompressionManager.NONE;
        }

        final AdmObjectNode storageBlockCompression =
                (AdmObjectNode) withObjectNode.get(DatasetDeclParametersUtil.STORAGE_BLOCK_COMPRESSION_PARAMETER_NAME);
        if (storageBlockCompression == null) {
            return null;
        }
        return storageBlockCompression
                .getOptionalString(DatasetDeclParametersUtil.STORAGE_BLOCK_COMPRESSION_SCHEME_PARAMETER_NAME);
    }

    public DatasetFormatInfo getDatasetFormatInfo(String defaultFormat, int defaultMaxTupleCount,
            double defaultFreeSpaceTolerance, int defaultMaxLeafNodeSize) throws CompilationException {
        if (datasetType != DatasetType.INTERNAL) {
            return DatasetFormatInfo.SYSTEM_DEFAULT;
        }

        AdmObjectNode datasetFormatNode = (AdmObjectNode) withObjectNode
                .getOrDefault(DatasetDeclParametersUtil.DATASET_FORMAT_PARAMETER_NAME, AdmObjectNode.EMPTY);
        DatasetConfig.DatasetFormat datasetFormat = DatasetConfig.DatasetFormat.getFormat(datasetFormatNode
                .getOptionalString(DatasetDeclParametersUtil.DATASET_FORMAT_FORMAT_PARAMETER_NAME, defaultFormat));

        if (datasetFormat == DatasetConfig.DatasetFormat.ROW) {
            return DatasetFormatInfo.SYSTEM_DEFAULT;
        }

        int maxTupleCount = datasetFormatNode.getOptionalInt(
                DatasetDeclParametersUtil.DATASET_FORMAT_MAX_TUPLE_COUNT_PARAMETER_NAME, defaultMaxTupleCount);
        double freeSpaceTolerance = datasetFormatNode.getOptionalDouble(
                DatasetDeclParametersUtil.DATASET_FORMAT_FREE_SPACE_TOLERANCE_PARAMETER_NAME,
                defaultFreeSpaceTolerance);
        String maxLeafNodeSizeString =
                datasetFormatNode.getOptionalString(DatasetDeclParametersUtil.DATASET_FORMAT_FREE_MAX_LEAF_NODE_SIZE);
        int maxLeafNodeSize = maxLeafNodeSizeString == null ? defaultMaxLeafNodeSize
                : (int) StorageUtil.getByteValue(maxLeafNodeSizeString);

        return new DatasetFormatInfo(datasetFormat, maxTupleCount, freeSpaceTolerance, maxLeafNodeSize);
    }

    public Map<String, String> getHints() {
        return hints;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.DATASET_DECL;
    }

    public IDatasetDetailsDecl getDatasetDetailsDecl() {
        return datasetDetailsDecl;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public AdmObjectNode getWithObjectNode() {
        return withObjectNode;
    }

}
