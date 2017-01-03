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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.commons.lang3.ObjectUtils;

public class InsertStatement implements IReturningStatement {

    private final Identifier dataverseName;
    private final Identifier datasetName;
    private final Query query;
    private final VariableExpr var;
    private Expression returnExpression;
    private int varCounter;

    public InsertStatement(Identifier dataverseName, Identifier datasetName, Query query, int varCounter,
            VariableExpr var, Expression returnExpression) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.query = query;
        this.varCounter = varCounter;
        this.var = var;
        this.returnExpression = returnExpression;
    }

    @Override
    public byte getKind() {
        return Statement.Kind.INSERT;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public void setVarCounter(int varCounter) {
        this.varCounter = varCounter;
    }

    @Override
    public List<Expression> getDirectlyEnclosedExpressions() {
        List<Expression> topLevelExpressions = new ArrayList<>();
        topLevelExpressions.add(query.getBody());
        if (returnExpression != null) {
            topLevelExpressions.add(returnExpression);
        }
        return topLevelExpressions;
    }

    @Override
    public boolean isTopLevel() {
        return true;
    }

    @Override
    public Expression getBody() {
        return query.getBody();
    }

    @Override
    public void setBody(Expression body) {
        query.setBody(body);
    }

    public VariableExpr getVar() {
        return var;
    }

    public Expression getReturnExpression() {
        return returnExpression;
    }

    public void setReturnExpression(Expression expr) {
        this.returnExpression = expr;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCodeMulti(datasetName, dataverseName, query, varCounter, var, returnExpression);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof InsertStatement)) {
            return false;
        }
        InsertStatement target = (InsertStatement) object;
        return ObjectUtils.equals(datasetName, target.datasetName)
                && ObjectUtils.equals(dataverseName, target.dataverseName) && ObjectUtils.equals(query, target.query)
                && ObjectUtils.equals(varCounter, target.varCounter) && ObjectUtils.equals(var, target.var)
                && ObjectUtils.equals(returnExpression, target.returnExpression);
    }

    @Override
    public byte getCategory() {
        if (var == null) {
            return Category.UPDATE;
        }
        return Category.QUERY;
    }

}
