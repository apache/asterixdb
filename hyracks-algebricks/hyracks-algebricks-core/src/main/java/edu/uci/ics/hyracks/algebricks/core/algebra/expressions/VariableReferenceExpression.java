/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.expressions;

import java.util.Collection;
import java.util.List;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class VariableReferenceExpression extends AbstractLogicalExpression {
    private LogicalVariable variable;

    public VariableReferenceExpression(LogicalVariable variable) {
        this.variable = variable;
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
        // if (!vars.contains(variable)) {
        vars.add(variable);
        // }
    }

    @Override
    public void substituteVar(LogicalVariable v1, LogicalVariable v2) {
        if (variable.equals(v1)) {
            variable = v2;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VariableReferenceExpression)) {
            return false;
        } else {
            return variable.equals(((VariableReferenceExpression) obj).getVariableReference());
        }
    }

    @Override
    public int hashCode() {
        return variable.getId();
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitVariableReferenceExpression(this, arg);
    }

    @Override
    public AbstractLogicalExpression cloneExpression() {
        return new VariableReferenceExpression(variable);
    }

    @Override
    public boolean splitIntoConjuncts(List<LogicalExpressionReference> conjs) {
        return false;
    }
}