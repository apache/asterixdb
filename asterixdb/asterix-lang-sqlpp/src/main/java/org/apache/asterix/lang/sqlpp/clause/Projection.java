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

    private Expression expr;
    private String name;
    private boolean star;
    private boolean varStar;

    public Projection(Expression expr, String name, boolean star, boolean varStar) {
        this.expr = expr;
        this.name = name;
        this.star = star;
        this.varStar = varStar;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean hasName() {
        return name != null;
    }

    public boolean star() {
        return star;
    }

    public boolean varStar() {
        return varStar;
    }

    @Override
    public String toString() {
        return star ? "*" : (String.valueOf(expr) + (varStar ? ".*" : (hasName() ? " as " + getName() : "")));
    }

    @Override
    public int hashCode() {
        return Objects.hash(expr, varStar, name, star);
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
        return Objects.equals(expr, target.expr) && Objects.equals(name, target.name) && varStar == target.varStar
                && star == target.star;
    }
}
