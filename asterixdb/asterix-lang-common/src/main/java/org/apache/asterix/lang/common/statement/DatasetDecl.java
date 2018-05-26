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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.util.MergePolicyUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class DatasetDecl extends AbstractStatement {
    protected static final String[] WITH_OBJECT_FIELDS = new String[] { MergePolicyUtils.MERGE_POLICY_PARAMETER_NAME };
    protected static final Set<String> WITH_OBJECT_FIELDS_SET = new HashSet<>(Arrays.asList(WITH_OBJECT_FIELDS));

    protected final Identifier name;
    protected final Identifier dataverse;
    protected final Identifier itemTypeDataverse;
    protected final Identifier itemTypeName;
    protected final Identifier metaItemTypeDataverse;
    protected final Identifier metaItemTypeName;
    protected final Identifier nodegroupName;
    protected final DatasetType datasetType;
    protected final IDatasetDetailsDecl datasetDetailsDecl;
    protected final Map<String, String> hints;
    private final AdmObjectNode withObjectNode;
    protected final boolean ifNotExists;

    public DatasetDecl(Identifier dataverse, Identifier name, Identifier itemTypeDataverse, Identifier itemTypeName,
            Identifier metaItemTypeDataverse, Identifier metaItemTypeName, Identifier nodeGroupName,
            Map<String, String> hints, DatasetType datasetType, IDatasetDetailsDecl idd, RecordConstructor withRecord,
            boolean ifNotExists) throws CompilationException {
        this.dataverse = dataverse;
        this.name = name;
        this.itemTypeName = itemTypeName;
        if (itemTypeDataverse.getValue() == null) {
            this.itemTypeDataverse = dataverse;
        } else {
            this.itemTypeDataverse = itemTypeDataverse;
        }
        this.metaItemTypeName = metaItemTypeName;
        if (metaItemTypeDataverse == null || metaItemTypeDataverse.getValue() == null) {
            this.metaItemTypeDataverse = dataverse;
        } else {
            this.metaItemTypeDataverse = metaItemTypeDataverse;
        }
        this.nodegroupName = nodeGroupName;
        this.hints = hints;
        try {
            this.withObjectNode = withRecord == null ? null : ExpressionUtils.toNode(withRecord);
        } catch (CompilationException e) {
            throw e;
        } catch (AlgebricksException e) {
            // TODO(tillw) make signatures throw Algebricks exceptions
            throw new CompilationException(e);
        }
        this.ifNotExists = ifNotExists;
        this.datasetType = datasetType;
        this.datasetDetailsDecl = idd;
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

    public Identifier getItemTypeName() {
        return itemTypeName;
    }

    public Identifier getItemTypeDataverse() {
        return itemTypeDataverse;
    }

    public String getQualifiedTypeName() {
        if (itemTypeDataverse == dataverse) {
            return itemTypeName.getValue();
        } else {
            return itemTypeDataverse.getValue() + "." + itemTypeName.getValue();
        }
    }

    public Identifier getMetaName() {
        return name;
    }

    public Identifier getMetaItemTypeName() {
        return metaItemTypeName == null ? new Identifier() : metaItemTypeName;
    }

    public Identifier getMetaItemTypeDataverse() {
        return metaItemTypeDataverse == null ? new Identifier() : metaItemTypeDataverse;
    }

    public String getQualifiedMetaTypeName() {
        if (metaItemTypeDataverse == dataverse) {
            return metaItemTypeName.getValue();
        } else {
            return metaItemTypeDataverse.getValue() + "." + metaItemTypeName.getValue();
        }
    }

    public Identifier getNodegroupName() {
        return nodegroupName;
    }

    public String getCompactionPolicy() throws CompilationException {
        AdmObjectNode mergePolicy = getMergePolicyObject();
        if (mergePolicy == null) {
            return null;
        }
        IAdmNode mergePolicyName = mergePolicy.get(MergePolicyUtils.MERGE_POLICY_NAME_PARAMETER_NAME);
        if (mergePolicyName == null) {
            throw new CompilationException(ErrorCode.WITH_FIELD_MUST_CONTAIN_SUB_FIELD,
                    MergePolicyUtils.MERGE_POLICY_PARAMETER_NAME, MergePolicyUtils.MERGE_POLICY_NAME_PARAMETER_NAME);
        }
        if (mergePolicyName.getType() != ATypeTag.STRING) {
            throw new CompilationException(ErrorCode.WITH_FIELD_MUST_BE_OF_TYPE,
                    MergePolicyUtils.MERGE_POLICY_PARAMETER_NAME + '.'
                            + MergePolicyUtils.MERGE_POLICY_NAME_PARAMETER_NAME,
                    ATypeTag.STRING);
        }
        return ((AdmStringNode) mergePolicyName).get();
    }

    private static AdmObjectNode validateWithObject(AdmObjectNode withObject) throws CompilationException {
        if (withObject == null) {
            return null;
        }
        for (String name : withObject.getFieldNames()) {
            if (!WITH_OBJECT_FIELDS_SET.contains(name)) {
                throw new CompilationException(ErrorCode.UNSUPPORTED_WITH_FIELD, name);
            }
        }
        return withObject;
    }

    private AdmObjectNode getMergePolicyObject() throws CompilationException {
        if (withObjectNode == null) {
            return null;
        }
        IAdmNode mergePolicy = validateWithObject(withObjectNode).get(MergePolicyUtils.MERGE_POLICY_PARAMETER_NAME);
        if (mergePolicy == null) {
            return null;
        }
        if (!mergePolicy.isObject()) {
            throw new CompilationException(ErrorCode.WITH_FIELD_MUST_BE_OF_TYPE,
                    MergePolicyUtils.MERGE_POLICY_PARAMETER_NAME, ATypeTag.OBJECT);
        }
        return (AdmObjectNode) mergePolicy;
    }

    public Map<String, String> getCompactionPolicyProperties() throws CompilationException {
        AdmObjectNode mergePolicy = getMergePolicyObject();
        if (mergePolicy == null) {
            return null;
        }
        IAdmNode mergePolicyParameters = mergePolicy.get(MergePolicyUtils.MERGE_POLICY_PARAMETERS_PARAMETER_NAME);
        if (mergePolicyParameters == null) {
            return null;
        }
        if (mergePolicyParameters.getType() != ATypeTag.OBJECT) {
            throw new CompilationException(ErrorCode.WITH_FIELD_MUST_BE_OF_TYPE,
                    MergePolicyUtils.MERGE_POLICY_PARAMETER_NAME + '.'
                            + MergePolicyUtils.MERGE_POLICY_PARAMETERS_PARAMETER_NAME,
                    ATypeTag.OBJECT);
        }
        return MergePolicyUtils.toProperties((AdmObjectNode) mergePolicyParameters);
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

    public Identifier getDataverse() {
        return dataverse;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public AdmObjectNode getWithObjectNode() {
        return withObjectNode;
    }

}
