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
package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class VariableReferenceExpression extends AbstractLogicalExpression {
    private int tupleRef;
    private LogicalVariable variable;

    public VariableReferenceExpression(int tupleRef, LogicalVariable variable) {
        if (variable == null) {
            throw new NullPointerException();
        }
        this.tupleRef = tupleRef;
        this.variable = variable;
    }

    public VariableReferenceExpression(LogicalVariable variable) {
        this(0, variable);
    }

    public VariableReferenceExpression(LogicalVariable variable, SourceLocation sourceLoc) {
        this(variable);
        this.sourceLoc = sourceLoc;
    }

    public LogicalVariable getVariableReference() {
        return variable;
    }

    public void setVariable(LogicalVariable variable) {
        this.variable = variable;
    }

    @Override
    public LogicalExpressionTag getExpressionTag() {
        return LogicalExpressionTag.VARIABLE;
    }

    @Override
    public String toString() {
        return variable.toString();
    }

    @Override
    public void getUsedVariables(Collection<LogicalVariable> vars) {
        vars.add(variable);
    }

    @Override
    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        if (variable.equals(v1)) {
            variable = v2;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VariableReferenceExpression) {
            final VariableReferenceExpression varRefExpr = (VariableReferenceExpression) obj;
            return tupleRef == varRefExpr.tupleRef && variable.equals(varRefExpr.getVariableReference());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return tupleRef + variable.hashCode();
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitVariableReferenceExpression(this, arg);
    }

    @Override
    public AbstractLogicalExpression cloneExpression() {
        VariableReferenceExpression varRef = new VariableReferenceExpression(variable);
        varRef.setSourceLocation(sourceLoc);
        return varRef;
    }

    @Override
    public boolean splitIntoConjuncts(List<Mutable<ILogicalExpression>> conjs) {
        return false;
    }
}
