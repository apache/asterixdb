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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmObjectNode;

public class CopyStatement extends AbstractStatement {

    private DataverseName dataverseName;
    private String datasetName;
    private ExternalDetailsDecl externalDetails;
    private AdmObjectNode withObjectNode;

    public CopyStatement(DataverseName dataverseName, String datasetName, ExternalDetailsDecl externalDetails,
            RecordConstructor withRecord) throws CompilationException {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.externalDetails = externalDetails;
        this.withObjectNode = withRecord == null ? new AdmObjectNode() : ExpressionUtils.toNode(withRecord);
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public void setDataverseName(DataverseName dataverseName) {
        this.dataverseName = dataverseName;
    }

    @Override
    public Kind getKind() {
        return Kind.COPY;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public ExternalDetailsDecl getExternalDetails() {
        return externalDetails;
    }

    public void setExternalDetails(ExternalDetailsDecl externalDetails) {
        this.externalDetails = externalDetails;
    }

    public AdmObjectNode getWithObjectNode() {
        return withObjectNode;
    }

    public void setWithObjectNode(AdmObjectNode withObjectNode) {
        this.withObjectNode = withObjectNode;
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }
}
