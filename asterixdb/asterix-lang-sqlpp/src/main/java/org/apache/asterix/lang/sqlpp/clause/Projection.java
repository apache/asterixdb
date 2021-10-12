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

package org.apache.asterix.lang.sqlpp.clause;

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class Projection extends AbstractClause {

    public enum Kind {
        NAMED_EXPR, // expr AS name
        STAR, // *
        VAR_STAR, // variable.*
        EVERY_VAR_STAR // *.* (currently only used in SQL-compatible mode)
    }

    private Kind kind;
    private Expression expr;
    private String name;

    public Projection(Kind kind, Expression expr, String name) {
        validateKind(kind, expr, name);
        this.kind = kind;
        this.expr = expr;
        this.name = name;
    }

    private static void validateKind(Kind kind, Expression expr, String name) {
        switch (kind) {
            case NAMED_EXPR:
                if (expr == null) {
                    throw new NullPointerException();
                }
                break;
            case STAR:
            case EVERY_VAR_STAR:
                if (expr != null || name != null) {
                    throw new IllegalArgumentException();
                }
                break;
            case VAR_STAR:
                if (expr == null) {
                    throw new NullPointerException();
                }
                if (name != null) {
                    throw new IllegalArgumentException();
                }
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    @Deprecated
    public Projection(Expression expr, String name, boolean star, boolean varStar) {
        this(asKind(star, varStar), expr, name);
    }

    @Deprecated
    private static Kind asKind(boolean star, boolean varStar) {
        return star ? Kind.STAR : varStar ? Kind.VAR_STAR : Kind.NAMED_EXPR;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.PROJECTION;
    }

    public Expression getExpression() {
        return expr;
    }

    public void setExpression(Expression expr) {
        this.expr = expr;
    }

    public boolean hasExpression() {
        return expr != null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean hasName() {
        return name != null;
    }

    public Kind getKind() {
        return kind;
    }

    public void setKind(Kind kind) {
        this.kind = kind;
    }

    @Deprecated
    public boolean star() {
        return kind == Kind.STAR;
    }

    @Deprecated
    public boolean varStar() {
        return kind == Kind.VAR_STAR;
    }

    @Override
    public String toString() {
        switch (kind) {
            case NAMED_EXPR:
                return expr + (hasName() ? " as " + getName() : "");
            case STAR:
                return "*";
            case VAR_STAR:
                return expr + ".*";
            case EVERY_VAR_STAR:
                return "*.*";
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, expr, name);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Projection)) {
            return false;
        }
        Projection target = (Projection) object;
        return kind == target.kind && Objects.equals(expr, target.expr) && Objects.equals(name, target.name);
    }
}
