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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.VectorIndexDeclUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.om.vector.VectorIndexParameters;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.OptionalBoolean;

public class CreateIndexStatement extends AbstractStatement {

    private final Namespace namespace;
    private final Identifier datasetName;
    private final Identifier indexName;
    private final IndexType indexType;
    private final List<IndexedElement> indexedElements;
    private final boolean enforced;
    private final boolean ifNotExists;
    // Specific to NGram indexes.
    private final int gramLength;
    // Specific to FullText indexes.
    private final String fullTextConfigName;
    private final OptionalBoolean excludeUnknownKey;
    private final OptionalBoolean castDefaultNull;
    private final Map<String, String> castConfig;

    private final List<CreateIndexStatement.IndexedElement> includeElements;
    private final VectorIndexParameters vectorParameters;

    public CreateIndexStatement(Namespace namespace, Identifier datasetName, Identifier indexName, IndexType indexType,
            List<IndexedElement> indexedElements, boolean enforced, int gramLength, String fullTextConfigName,
            boolean ifNotExists, Boolean excludeUnknownKey, Boolean castDefaultNull, Map<String, String> castConfig,
            List<CreateIndexStatement.IndexedElement> includeElements, RecordConstructor withObjectRecord)
            throws CompilationException {
        this.namespace = namespace;
        this.datasetName = Objects.requireNonNull(datasetName);
        this.indexName = Objects.requireNonNull(indexName);
        this.indexType = Objects.requireNonNull(indexType);
        this.indexedElements = Objects.requireNonNull(indexedElements);
        this.enforced = enforced;
        this.gramLength = gramLength;
        this.ifNotExists = ifNotExists;
        this.fullTextConfigName = fullTextConfigName;
        this.excludeUnknownKey = OptionalBoolean.ofNullable(excludeUnknownKey);
        this.castDefaultNull = OptionalBoolean.ofNullable(castDefaultNull);
        this.castConfig = castConfig == null ? Collections.emptyMap() : castConfig;
        this.includeElements = includeElements;
        // The WITH clause is vector-index-only (the grammar rejects it on any other index type), and a
        // vector index always has one, so this is non-null exactly for TYPE VTREE.
        this.vectorParameters =
                indexType == IndexType.VTREE ? VectorIndexDeclUtil.validateAndGetParameters(withObjectRecord) : null;
    }

    public String getFullTextConfigName() {
        return fullTextConfigName;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Identifier getIndexName() {
        return indexName;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public List<IndexedElement> getIndexedElements() {
        return indexedElements;
    }

    public List<IndexedElement> getIncludeElements() {
        return includeElements;
    }

    public boolean isEnforced() {
        return enforced;
    }

    public boolean hasExcludeUnknownKey() {
        return excludeUnknownKey.isPresent();
    }

    public OptionalBoolean getExcludeUnknownKey() {
        return excludeUnknownKey;
    }

    public boolean hasCastDefaultNull() {
        return castDefaultNull.isPresent();
    }

    public OptionalBoolean getCastDefaultNull() {
        return castDefaultNull;
    }

    public int getGramLength() {
        return gramLength;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    public Map<String, String> getCastConfig() {
        return castConfig;
    }

    /**
     * The validated {@code WITH} parameters of a {@code TYPE VTREE} index, never {@code null} for that index
     * type; {@code null} for every other type, none of which accepts a {@code WITH} clause.
     */
    public VectorIndexParameters getVectorParameters() {
        return vectorParameters;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.CREATE_INDEX;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public static final class IndexedElement {

        private final int sourceIndicator;

        private final List<List<String>> unnestList;

        private final List<Pair<List<String>, IndexedTypeExpression>> projectList;

        private SourceLocation sourceLoc;

        public IndexedElement(int sourceIndicator, List<List<String>> unnestList,
                List<Pair<List<String>, IndexedTypeExpression>> projectList) {
            if (Objects.requireNonNull(projectList).isEmpty()) {
                throw new IllegalArgumentException();
            }
            this.sourceIndicator = sourceIndicator;
            this.unnestList = unnestList != null ? unnestList : Collections.emptyList();
            this.projectList = projectList;
        }

        public int getSourceIndicator() {
            return sourceIndicator;
        }

        public boolean hasUnnest() {
            return !unnestList.isEmpty();
        }

        public List<List<String>> getUnnestList() {
            return unnestList;
        }

        public List<Pair<List<String>, IndexedTypeExpression>> getProjectList() {
            return projectList;
        }

        public Triple<Integer, List<List<String>>, List<List<String>>> toIdentifier() {
            List<List<String>> newProjectList = projectList.stream().map(Pair::getFirst).collect(Collectors.toList());
            return new Triple<>(sourceIndicator, unnestList, newProjectList);
        }

        public SourceLocation getSourceLocation() {
            return sourceLoc;
        }

        public void setSourceLocation(SourceLocation sourceLoc) {
            this.sourceLoc = sourceLoc;
        }

        public String getProjectListDisplayForm() {
            return projectList.stream().map(Pair::getFirst).map(String::valueOf).collect(Collectors.joining(", "));
        }
    }
}
