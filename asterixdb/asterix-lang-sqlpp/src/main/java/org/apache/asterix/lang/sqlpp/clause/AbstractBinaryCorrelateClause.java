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

import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.sqlpp.optype.JoinType;

public abstract class AbstractBinaryCorrelateClause extends AbstractClause {

    private JoinType joinType;
    private Expression rightExpr;
    private VariableExpr rightVar;
    private VariableExpr rightPosVar;

    public AbstractBinaryCorrelateClause(JoinType joinType, Expression rightExpr, VariableExpr rightVar,
            VariableExpr rightPosVar) {
        this.joinType = joinType;
        this.rightExpr = rightExpr;
        this.rightVar = rightVar;
        this.rightPosVar = rightPosVar;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Expression getRightExpression() {
        return rightExpr;
    }

    public void setRightExpression(Expression rightExpr) {
        this.rightExpr = rightExpr;
    }

    public VariableExpr getRightVariable() {
        return rightVar;
    }

    public VariableExpr getPositionalVariable() {
        return rightPosVar;
    }

    public boolean hasPositionalVariable() {
        return rightPosVar != null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, rightExpr, rightPosVar, rightVar);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof AbstractBinaryCorrelateClause)) {
            return false;
        }
        AbstractBinaryCorrelateClause target = (AbstractBinaryCorrelateClause) object;
        return Objects.equals(joinType, target.joinType) && Objects.equals(rightExpr, target.rightExpr)
                && Objects.equals(rightPosVar, target.rightPosVar) && Objects.equals(rightVar, target.rightVar);
    }

}
