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
import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.IReturningStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class Query extends AbstractStatement implements IReturningStatement {
    private boolean explain;
    private boolean topLevel = true;
    private Expression body;
    private int varCounter;

    public Query() {
        this(false);
    }

    public Query(boolean explain) {
        this.explain = explain;
    }

    public Query(boolean explain, boolean topLevel, Expression body, int varCounter) {
        this.explain = explain;
        this.topLevel = topLevel;
        this.body = body;
        this.varCounter = varCounter;
    }

    @Override
    public Expression getBody() {
        return body;
    }

    @Override
    public void setBody(Expression body) {
        this.body = body;
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
        return Collections.singletonList(body);
    }

    public void setTopLevel(boolean topLevel) {
        this.topLevel = topLevel;
    }

    @Override
    public boolean isTopLevel() {
        return topLevel;
    }

    public void setExplain(boolean explain) {
        this.explain = explain;
    }

    public boolean isExplain() {
        return explain;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.QUERY;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(body, topLevel, explain, varCounter);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Query)) {
            return false;
        }
        Query target = (Query) object;
        return explain == target.explain && Objects.equals(body, target.body) && topLevel == target.topLevel
                && varCounter == target.varCounter;
    }

    @Override
    public byte getCategory() {
        return Category.QUERY;
    }

    @Override
    public String toString() {
        return body.toString();
    }
}
