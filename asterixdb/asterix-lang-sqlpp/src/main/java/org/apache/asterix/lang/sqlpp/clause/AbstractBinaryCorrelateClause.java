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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.TimeTravel;

public abstract class AbstractBinaryCorrelateClause extends AbstractClause {

    private Expression rightExpr;
    private final VariableExpr rightVar;
    private final VariableExpr rightPosVar;
    private final TimeTravel timeTravel;

    public AbstractBinaryCorrelateClause(Expression rightExpr, VariableExpr rightVar, VariableExpr rightPosVar) {
        this(rightExpr, rightVar, rightPosVar, null);
    }

    public AbstractBinaryCorrelateClause(Expression rightExpr, VariableExpr rightVar, VariableExpr rightPosVar,
            TimeTravel timeTravel) {
        this.rightExpr = rightExpr;
        this.rightVar = rightVar;
        this.rightPosVar = rightPosVar;
        this.timeTravel = timeTravel;
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

    public boolean hasTimeTravel() {
        return timeTravel != null;
    }

    public TimeTravel getTimeTravel() {
        return timeTravel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rightExpr, rightPosVar, rightVar, timeTravel);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof AbstractBinaryCorrelateClause target)) {
            return false;
        }
        return Objects.equals(rightExpr, target.rightExpr) && Objects.equals(rightPosVar, target.rightPosVar)
                && Objects.equals(rightVar, target.rightVar) && Objects.equals(timeTravel, target.timeTravel);
    }

}
