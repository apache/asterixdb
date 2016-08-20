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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.commons.lang3.ObjectUtils;

public class DeleteStatement implements Statement {

    private VariableExpr vars;
    private Identifier dataverseName;
    private Identifier datasetName;
    private Expression condition;
    private int varCounter;
    private List<String> dataverses;
    private List<String> datasets;
    private Query rewrittenQuery;

    public DeleteStatement(VariableExpr vars, Identifier dataverseName, Identifier datasetName, Expression condition,
            int varCounter, List<String> dataverses, List<String> datasets) {
        this.vars = vars;
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.condition = condition;
        this.varCounter = varCounter;
        this.dataverses = dataverses;
        this.datasets = datasets;
    }

    @Override
    public byte getKind() {
        return Statement.Kind.DELETE;
    }

    public VariableExpr getVariableExpr() {
        return vars;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Expression getCondition() {
        return condition;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public void setQuery(Query rewrittenQuery) {
        this.rewrittenQuery = rewrittenQuery;
    }

    public Query getQuery() {
        return rewrittenQuery;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    public List<String> getDataverses() {
        return dataverses;
    }

    public List<String> getDatasets() {
        return datasets;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(condition, datasetName, datasets, dataverseName, dataverses, rewrittenQuery,
                vars);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof DeleteStatement)) {
            return false;
        }
        DeleteStatement target = (DeleteStatement) object;
        boolean equals = ObjectUtils.equals(condition, target.condition)
                && ObjectUtils.equals(datasetName, target.datasetName) && ObjectUtils.equals(datasets, target.datasets)
                && ObjectUtils.equals(dataverseName, target.dataverseName);
        return equals && ObjectUtils.equals(dataverses, target.dataverses)
                && ObjectUtils.equals(rewrittenQuery, target.rewrittenQuery) && ObjectUtils.equals(vars, target.vars);
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }
}
