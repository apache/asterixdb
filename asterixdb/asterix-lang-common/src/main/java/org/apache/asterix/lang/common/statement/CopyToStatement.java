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

import static org.apache.asterix.lang.common.base.Statement.Category.QUERY;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.clause.OrderbyClause;
import org.apache.asterix.lang.common.expression.LiteralExpr;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.literal.StringLiteral;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class CopyToStatement extends AbstractStatement implements IReturningStatement {
    private final String datasetName;
    private final VariableExpr sourceVariable;
    private final ExternalDetailsDecl externalDetailsDecl;
    private final Map<Integer, VariableExpr> partitionsVariables;
    private final List<OrderbyClause.OrderModifier> orderByModifiers;
    private final List<OrderbyClause.NullOrderModifier> orderByNullModifierList;

    private Namespace namespace;
    private Query query;
    private List<Expression> pathExpressions;

    private List<Expression> partitionExpressions;
    private List<Expression> orderByList;
    private int varCounter;

    public CopyToStatement(Namespace namespace, String datasetName, Query query, VariableExpr sourceVariable,
            ExternalDetailsDecl externalDetailsDecl, List<Expression> pathExpressions,
            List<Expression> partitionExpressions, Map<Integer, VariableExpr> partitionsVariables,
            List<Expression> orderbyList, List<OrderbyClause.OrderModifier> orderByModifiers,
            List<OrderbyClause.NullOrderModifier> orderByNullModifierList, int varCounter) {
        this.namespace = namespace;
        this.datasetName = datasetName;
        this.query = query;
        this.sourceVariable = sourceVariable;
        this.externalDetailsDecl = externalDetailsDecl;
        this.pathExpressions = pathExpressions;
        this.partitionExpressions = partitionExpressions;
        this.partitionsVariables = partitionsVariables;
        this.orderByList = orderbyList;
        this.orderByModifiers = orderByModifiers;
        this.orderByNullModifierList = orderByNullModifierList;
        this.varCounter = varCounter;

        if (pathExpressions.isEmpty()) {
            // Ensure path expressions to have at least an empty string
            pathExpressions.add(new LiteralExpr(new StringLiteral("")));
        }
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public Kind getKind() {
        return Kind.COPY_TO;
    }

    @Override
    public byte getCategory() {
        return QUERY;
    }

    public void setNamespace(Namespace namespace) {
        this.namespace = namespace;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }

    public VariableExpr getSourceVariable() {
        return sourceVariable;
    }

    public ExternalDetailsDecl getExternalDetailsDecl() {
        return externalDetailsDecl;
    }

    public List<Expression> getPathExpressions() {
        return pathExpressions;
    }

    public void setPathExpressions(List<Expression> pathExpressions) {
        if (pathExpressions.isEmpty()) {
            pathExpressions.add(new LiteralExpr(new StringLiteral("")));
        }
        this.pathExpressions = pathExpressions;
    }

    public List<Expression> getPartitionExpressions() {
        return partitionExpressions;
    }

    public void setPartitionExpressions(List<Expression> partitionExpressions) {
        this.partitionExpressions = partitionExpressions;
    }

    public Map<Integer, VariableExpr> getPartitionsVariables() {
        return partitionsVariables;
    }

    public List<Expression> getOrderByList() {
        return orderByList;
    }

    public void setOrderByList(List<Expression> orderbyList) {
        this.orderByList = orderbyList;
    }

    public List<OrderbyClause.OrderModifier> getOrderByModifiers() {
        return orderByModifiers;
    }

    public List<OrderbyClause.NullOrderModifier> getOrderByNullModifierList() {
        return orderByNullModifierList;
    }

    public boolean hasOverClause() {
        return hasPartitionClause() || hasOrderClause();
    }

    public boolean hasPartitionClause() {
        return !partitionExpressions.isEmpty();
    }

    public boolean hasOrderClause() {
        return !orderByList.isEmpty();
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
    public boolean isTopLevel() {
        return true;
    }

    @Override
    public List<Expression> getDirectlyEnclosedExpressions() {
        List<Expression> topLevelExpressions = new ArrayList<>();
        topLevelExpressions.add(query.getBody());
        topLevelExpressions.addAll(pathExpressions);
        topLevelExpressions.addAll(partitionExpressions);
        topLevelExpressions.addAll(orderByList);
        return topLevelExpressions;
    }

    @Override
    public Expression getBody() {
        return query.getBody();
    }

    @Override
    public void setBody(Expression expr) {
        query.setBody(expr);
    }
}
