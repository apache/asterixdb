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
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.util.FullTextUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmArrayNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.google.common.collect.ImmutableList;

public class CreateFullTextFilterStatement extends AbstractStatement {

    private final Namespace namespace;
    private final String filterName;
    private final boolean ifNotExists;
    private final AdmObjectNode filterNode;

    public static final String FIELD_NAME_TYPE = "type";
    public static final String FIELD_NAME_STOPWORDS_LIST = "stopwordsList";
    public static final String FIELD_TYPE_STOPWORDS = "stopwords";

    public CreateFullTextFilterStatement(Namespace namespace, String filterName, boolean ifNotExists,
            RecordConstructor expr) throws CompilationException {
        this.namespace = namespace;
        this.filterName = filterName;
        this.ifNotExists = ifNotExists;
        this.filterNode = FullTextUtil.getFilterNode(expr);
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public String getFilterName() {
        return filterName;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    public String getFilterType() throws HyracksDataException {
        return filterNode.getString(FIELD_NAME_TYPE);
    }

    public ImmutableList<String> getStopwordsList() throws CompilationException {
        ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
        AdmArrayNode arrayNode = (AdmArrayNode) filterNode.get(FIELD_NAME_STOPWORDS_LIST);

        for (IAdmNode node : arrayNode) {
            if (!(node instanceof AdmStringNode)) {
                throw new CompilationException(ErrorCode.PARSE_ERROR, getSourceLocation(),
                        "error when parsing stopwords list");
            }

            listBuilder.add(((AdmStringNode) node).get());
        }

        return listBuilder.build();
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_FULL_TEXT_FILTER;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }
}
