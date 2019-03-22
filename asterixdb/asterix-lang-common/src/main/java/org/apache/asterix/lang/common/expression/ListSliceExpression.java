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
package org.apache.asterix.lang.common.expression;

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class ListSliceExpression extends AbstractAccessor {
    private Expression startIndexExpression;
    private Expression endIndexExpression;

    public ListSliceExpression(Expression expr, Expression startIndexExpression, Expression endIndexExpression) {
        super(expr);
        this.startIndexExpression = startIndexExpression;
        this.endIndexExpression = endIndexExpression;
    }

    public Expression getStartIndexExpression() {
        return startIndexExpression;
    }

    public Expression getEndIndexExpression() {
        return endIndexExpression;
    }

    // Only end expression can be null (Value not provided)
    public boolean hasEndExpression() {
        return endIndexExpression != null;
    }

    public void setStartIndexExpression(Expression startIndexExpression) {
        this.startIndexExpression = startIndexExpression;
    }

    public void setEndIndexExpression(Expression endIndexExpression) {
        this.endIndexExpression = endIndexExpression;
    }

    @Override
    public Kind getKind() {
        return Kind.LIST_SLICE_EXPRESSION;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, startIndexExpression, endIndexExpression);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ListSliceExpression)) {
            return false;
        }
        ListSliceExpression target = (ListSliceExpression) object;
        return super.equals(target) && Objects.equals(startIndexExpression, target.startIndexExpression)
                && Objects.equals(endIndexExpression, target.endIndexExpression);
    }

    @Override
    public String toString() {
        return expr + "[" + (startIndexExpression + ":" + (hasEndExpression() ? endIndexExpression : "")) + "]";
    }
}
