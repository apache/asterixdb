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
import org.apache.hyracks.storage.am.lsm.invertedindex.fulltext.TokenizerCategory;

import com.google.common.collect.ImmutableList;

public class CreateFullTextConfigStatement extends AbstractStatement {

    private final Namespace namespace;
    private final String configName;
    private final boolean ifNotExists;
    private final AdmObjectNode configNode;

    public static final String FIELD_NAME_TOKENIZER = "tokenizer";
    public static final String FIELD_NAME_FILTER_PIPELINE = "filterPipeline";

    public CreateFullTextConfigStatement(Namespace namespace, String configName, boolean ifNotExists,
            RecordConstructor expr) throws CompilationException {
        this.namespace = namespace;
        this.configName = configName;
        this.ifNotExists = ifNotExists;
        this.configNode = FullTextUtil.validateAndGetConfigNode(expr);
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public String getConfigName() {
        return configName;
    }

    public boolean getIfNotExists() {
        return ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_FULL_TEXT_CONFIG;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public TokenizerCategory getTokenizerCategory() throws HyracksDataException {
        String tokenizerCategoryStr = configNode.getString(FIELD_NAME_TOKENIZER);
        return TokenizerCategory.getEnumIgnoreCase(tokenizerCategoryStr);
    }

    public ImmutableList<String> getFilterNames() {
        AdmArrayNode arrayNode = (AdmArrayNode) configNode.get(FIELD_NAME_FILTER_PIPELINE);
        ImmutableList.Builder<String> filterNamesBuilder = ImmutableList.builder();

        for (IAdmNode iAdmNode : arrayNode) {
            filterNamesBuilder.add(((AdmStringNode) iAdmNode).get());
        }

        return filterNamesBuilder.build();
    }

}
