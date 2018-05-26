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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.IndexedTypeExpression;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class CreateIndexStatement extends AbstractStatement {

    private Identifier indexName;
    private Identifier dataverseName;
    private Identifier datasetName;
    private List<Pair<List<String>, IndexedTypeExpression>> fieldExprs = new ArrayList<>();
    private List<Integer> fieldIndexIndicators = new ArrayList<>();
    private IndexType indexType = IndexType.BTREE;
    private boolean enforced;
    private boolean ifNotExists;

    // Specific to NGram indexes.
    private int gramLength = -1;

    public CreateIndexStatement() {
    }

    public void setGramLength(int gramLength) {
        this.gramLength = gramLength;
    }

    public int getGramLength() {
        return gramLength;
    }

    public Identifier getIndexName() {
        return indexName;
    }

    public void setIndexName(Identifier indexName) {
        this.indexName = indexName;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public void setDataverseName(Identifier dataverseName) {
        this.dataverseName = dataverseName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(Identifier datasetName) {
        this.datasetName = datasetName;
    }

    public List<Pair<List<String>, IndexedTypeExpression>> getFieldExprs() {
        return fieldExprs;
    }

    public void addFieldExprPair(Pair<List<String>, IndexedTypeExpression> fp) {
        this.fieldExprs.add(fp);
    }

    public List<Integer> getFieldSourceIndicators() {
        return fieldIndexIndicators;
    }

    public void addFieldIndexIndicator(Integer index) {
        fieldIndexIndicators.add(index);
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public boolean isEnforced() {
        return enforced;
    }

    public void setEnforced(boolean isEnforced) {
        this.enforced = isEnforced;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.CREATE_INDEX;
    }

    public boolean hasMetaField() {
        if (fieldIndexIndicators != null) {
            for (Integer indicator : fieldIndexIndicators) {
                if (indicator.intValue() != 0) {
                    return true;
                }
            }
        }
        return false;
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
