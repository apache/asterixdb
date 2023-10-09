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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public final class CreateViewStatement extends AbstractStatement {

    private final Namespace namespace;

    private final String viewName;

    private final TypeExpression itemType;

    private final String viewBody;

    private final Expression viewBodyExpression;

    private final Map<String, String> viewConfig;

    private final KeyDecl primaryKeyDecl;

    private final List<ForeignKeyDecl> foreignKeyDecls;

    private final Boolean defaultNull;

    private final boolean replaceIfExists;

    private final boolean ifNotExists;

    public CreateViewStatement(Namespace namespace, String viewName, TypeExpression itemType, String viewBody,
            Expression viewBodyExpression, Boolean defaultNull, Map<String, String> viewConfig, KeyDecl primaryKeyDecl,
            List<ForeignKeyDecl> foreignKeyDecls, boolean replaceIfExists, boolean ifNotExists) {
        this.namespace = namespace;
        this.viewName = Objects.requireNonNull(viewName);
        this.itemType = itemType;
        this.viewBody = Objects.requireNonNull(viewBody);
        this.viewBodyExpression = Objects.requireNonNull(viewBodyExpression);
        this.defaultNull = defaultNull;
        this.viewConfig = viewConfig;
        this.primaryKeyDecl = primaryKeyDecl;
        this.foreignKeyDecls = foreignKeyDecls;
        this.replaceIfExists = replaceIfExists;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.CREATE_VIEW;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public String getViewName() {
        return viewName;
    }

    public boolean hasItemType() {
        return itemType != null;
    }

    public TypeExpression getItemType() {
        return itemType;
    }

    public String getViewBody() {
        return viewBody;
    }

    public Expression getViewBodyExpression() {
        return viewBodyExpression;
    }

    public boolean getReplaceIfExists() {
        return replaceIfExists;
    }

    public boolean getIfNotExists() {
        return ifNotExists;
    }

    // Typed view parameters

    public Boolean getDefaultNull() {
        return defaultNull;
    }

    public KeyDecl getPrimaryKeyDecl() {
        return primaryKeyDecl;
    }

    public List<ForeignKeyDecl> getForeignKeyDecls() {
        return foreignKeyDecls;
    }

    public Map<String, String> getViewConfiguration() {
        return viewConfig;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    public static class KeyDecl {

        protected final List<List<String>> fields;

        protected final List<Integer> sourceIndicators;

        public KeyDecl(List<List<String>> fields, List<Integer> sourceIndicators) {
            this.fields = fields;
            this.sourceIndicators = sourceIndicators;
        }

        public List<List<String>> getFields() {
            return fields;
        }

        public List<Integer> getSourceIndicators() {
            return sourceIndicators;
        }
    }

    public static class ForeignKeyDecl extends KeyDecl {

        private final Namespace referencedNamespace;

        private final Identifier referencedDatasetName;

        public ForeignKeyDecl(List<List<String>> fields, List<Integer> sourceIndicators, Namespace referencedNamespace,
                Identifier referencedDatasetName) {
            super(fields, sourceIndicators);
            this.referencedNamespace = referencedNamespace;
            this.referencedDatasetName = referencedDatasetName;
        }

        public String getReferencedDatabaseName() {
            return referencedNamespace == null ? null : referencedNamespace.getDatabaseName();
        }

        public DataverseName getReferencedDataverseName() {
            return referencedNamespace == null ? null : referencedNamespace.getDataverseName();
        }

        public Identifier getReferencedDatasetName() {
            return referencedDatasetName;
        }
    }
}
