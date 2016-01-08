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

import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.bootstrap.MetadataConstants;

public class DatasetDecl implements Statement {
    protected final Identifier name;
    protected final Identifier dataverse;
    protected final Identifier itemTypeDataverse;
    protected final Identifier itemTypeName;
    protected final Identifier nodegroupName;
    protected final String compactionPolicy;
    protected final Map<String, String> compactionPolicyProperties;
    protected final DatasetType datasetType;
    protected final IDatasetDetailsDecl datasetDetailsDecl;
    protected final Map<String, String> hints;
    protected final boolean ifNotExists;

    public DatasetDecl(Identifier dataverse, Identifier name, Identifier itemTypeDataverse, Identifier itemTypeName,
            Identifier nodeGroupName, String compactionPolicy, Map<String, String> compactionPolicyProperties,
            Map<String, String> hints, DatasetType datasetType, IDatasetDetailsDecl idd, boolean ifNotExists) {
        this.dataverse = dataverse;
        this.name = name;
        this.itemTypeName = itemTypeName;
        if (itemTypeDataverse.getValue() == null) {
            this.itemTypeDataverse = dataverse;
        } else {
            this.itemTypeDataverse = itemTypeDataverse;
        }
        this.nodegroupName = nodeGroupName == null ? new Identifier(MetadataConstants.METADATA_DEFAULT_NODEGROUP_NAME)
                : nodeGroupName;
        this.compactionPolicy = compactionPolicy;
        this.compactionPolicyProperties = compactionPolicyProperties;
        this.hints = hints;
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

    public Identifier getNodegroupName() {
        return nodegroupName;
    }

    public String getCompactionPolicy() {
        return compactionPolicy;
    }

    public Map<String, String> getCompactionPolicyProperties() {
        return compactionPolicyProperties;
    }

    public Map<String, String> getHints() {
        return hints;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    @Override
    public Kind getKind() {
        return Kind.DATASET_DECL;
    }

    public IDatasetDetailsDecl getDatasetDetailsDecl() {
        return datasetDetailsDecl;
    }

    public Identifier getDataverse() {
        return dataverse;
    }

}
