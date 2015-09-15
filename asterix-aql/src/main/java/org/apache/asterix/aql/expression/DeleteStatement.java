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
package org.apache.asterix.aql.expression;

import java.util.List;

import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;

public class DeleteStatement implements Statement {

    private VariableExpr vars;
    private Identifier dataverseName;
    private Identifier datasetName;
    private Expression condition;
    private int varCounter;
    private List<String> dataverses;
    private List<String> datasets;

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
    public Kind getKind() {
        return Kind.DELETE;
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

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitDeleteStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public List<String> getDataverses() {
        return dataverses;
    }

    public List<String> getDatasets() {
        return datasets;
    }

}