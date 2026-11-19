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

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

/**
 * An UPDATE statement. Its query is not built by the parser: the statement rewriter builds it from the change
 * sequence and condition once the collection has been resolved, so {@link #getQuery()} is {@code null} until then.
 */
public class UpdateStatement extends InsertStatement {

    private final Expression changeSeq;
    private final Expression condition;

    public UpdateStatement(Namespace namespace, String datasetName, VariableExpr aliasVar, Expression changeSeq,
            Expression condition, int varCounter, Expression returnExpression) {
        super(namespace, datasetName, null, varCounter, aliasVar, returnExpression);
        this.changeSeq = changeSeq;
        this.condition = condition;
    }

    public Expression getChangeSeq() {
        return changeSeq;
    }

    public Expression getCondition() {
        return condition;
    }

    @Override
    public void setQuery(Query query) {
        super.setQuery(query);
    }

    @Override
    public Kind getKind() {
        return Kind.UPDATE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Kind.UPDATE, changeSeq, condition);
    }

    @Override
    @SuppressWarnings("squid:S1067") // expressions should not be too complex
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof UpdateStatement) || !super.equals(object)) {
            return false;
        }
        UpdateStatement target = (UpdateStatement) object;
        return Objects.equals(changeSeq, target.changeSeq) && Objects.equals(condition, target.condition);
    }
}
