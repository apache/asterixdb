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
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Literal;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.optype.UnnestType;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class UnnestClause extends AbstractBinaryCorrelateClause {

    private final UnnestType unnestType;

    private Literal.Type outerUnnestMissingValueType;

    public UnnestClause(UnnestType unnestType, Expression rightExpr, VariableExpr rightVar, VariableExpr rightPosVar,
            Literal.Type outerUnnestMissingValueType) {
        super(rightExpr, rightVar, rightPosVar);
        this.unnestType = unnestType;
        setOuterUnnestMissingValueType(outerUnnestMissingValueType);
    }

    public Literal.Type getOuterUnnestMissingValueType() {
        return outerUnnestMissingValueType;
    }

    public void setOuterUnnestMissingValueType(Literal.Type outerUnnestMissingValueType) {
        this.outerUnnestMissingValueType = validateMissingValueType(unnestType, outerUnnestMissingValueType);
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.UNNEST_CLAUSE;
    }

    public UnnestType getUnnestType() {
        return unnestType;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + 31 * unnestType.hashCode()
                + +(outerUnnestMissingValueType != null ? outerUnnestMissingValueType.hashCode() : 0);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof UnnestClause)) {
            return false;
        }
        UnnestClause target = (UnnestClause) object;
        return super.equals(target) && unnestType.equals(target.getUnnestType())
                && Objects.equals(outerUnnestMissingValueType, target.outerUnnestMissingValueType);
    }

    private static Literal.Type validateMissingValueType(UnnestType unnestType, Literal.Type missingValueType) {
        switch (unnestType) {
            case INNER:
                if (missingValueType != null) {
                    throw new IllegalArgumentException(String.valueOf(missingValueType));
                }
                return null;
            case LEFTOUTER:
                switch (Objects.requireNonNull(missingValueType)) {
                    case MISSING:
                    case NULL:
                        return missingValueType;
                    default:
                        throw new IllegalArgumentException(String.valueOf(missingValueType));
                }
            default:
                throw new IllegalStateException(String.valueOf(unnestType));
        }
    }
}
