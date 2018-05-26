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
package org.apache.asterix.lang.common.clause;

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class LimitClause extends AbstractClause {
    private Expression limitExpr;
    private Expression offset;

    public LimitClause() {
        // Default constructor.
    }

    public LimitClause(Expression limitexpr, Expression offset) {
        this.limitExpr = limitexpr;
        this.offset = offset;
    }

    public Expression getLimitExpr() {
        return limitExpr;
    }

    public void setLimitExpr(Expression limitexpr) {
        this.limitExpr = limitexpr;
    }

    public Expression getOffset() {
        return offset;
    }

    public void setOffset(Expression offset) {
        this.offset = offset;
    }

    public boolean hasOffset() {
        return offset != null;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.LIMIT_CLAUSE;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limitExpr, offset);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LimitClause)) {
            return false;
        }
        LimitClause target = (LimitClause) object;
        return limitExpr.equals(target.getLimitExpr()) && Objects.equals(offset, target.getOffset());
    }
}
