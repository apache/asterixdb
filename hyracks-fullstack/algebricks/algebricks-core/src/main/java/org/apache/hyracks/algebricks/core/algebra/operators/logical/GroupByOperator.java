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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class GroupByOperator extends AbstractOperatorWithNestedPlans {
    // If the LogicalVariable in a pair is null, it means that the GroupBy is
    // only grouping by the expression, without producing a new variable.
    private final List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gByList;
    private final List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList;

    // In decorList, if the variable (first member of the pair) is null, the
    // second member of the pair is variable reference which is propagated.

    private boolean groupAll = false;
    private boolean global = true;

    public GroupByOperator() {
        super();
        gByList = new ArrayList<>();
        decorList = new ArrayList<>();
    }

    public GroupByOperator(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList, List<ILogicalPlan> nestedPlans) {
        this(groupByList, decorList, nestedPlans, false);
    }

    public GroupByOperator(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> groupByList,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> decorList, List<ILogicalPlan> nestedPlans,
            boolean groupAll) {
        super(nestedPlans);
        this.decorList = decorList;
        this.gByList = groupByList;
        this.groupAll = groupAll;
        checkGroupAll(groupAll);
    }

    public void addGbyExpression(LogicalVariable variable, ILogicalExpression expression) {
        this.gByList.add(new Pair<>(variable, new MutableObject<>(expression)));
    }

    public void addDecorExpression(LogicalVariable variable, ILogicalExpression expression) {
        this.decorList.add(new Pair<>(variable, new MutableObject<>(expression)));
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.GROUP;
    }

    public List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> getGroupByList() {
        return gByList;
    }

    public List<LogicalVariable> getGroupByVarList() {
        List<LogicalVariable> varList = new ArrayList<>(gByList.size());
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : gByList) {
            ILogicalExpression expr = ve.second.getValue();
            if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression v = (VariableReferenceExpression) expr;
                varList.add(v.getVariableReference());
            }
        }
        return varList;
    }

    public static String veListToString(List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> vePairList) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean fst = true;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> ve : vePairList) {
            if (fst) {
                fst = false;
            } else {
                sb.append("; ");
            }
            if (ve.first != null) {
                sb.append(ve.first + " := " + ve.second);
            } else {
                sb.append(ve.second.getValue());
            }
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void recomputeSchema() {
        super.recomputeSchema();
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            schema.add(p.first);
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            schema.add(getDecorVariable(p));
        }
    }

    @Override
    public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            if (p.first != null) {
                vars.add(p.first);
            }
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            if (p.first != null) {
                vars.add(p.first);
            }
        }
    }

    @Override
    public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> g : gByList) {
            g.second.getValue().getUsedVariables(vars);
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> g : decorList) {
            g.second.getValue().getUsedVariables(vars);
        }
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return new VariablePropagationPolicy() {

            @Override
            public void propagateVariables(IOperatorSchema target, IOperatorSchema... sources)
                    throws AlgebricksException {
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
                    ILogicalExpression expr = p.second.getValue();
                    if (p.first != null) {
                        target.addVariable(p.first);
                    } else {
                        if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                            throw new AlgebricksException("hash group-by expects variable references.");
                        }
                        VariableReferenceExpression v = (VariableReferenceExpression) expr;
                        target.addVariable(v.getVariableReference());
                    }
                }
                for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
                    ILogicalExpression expr = p.second.getValue();
                    if (expr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                        throw new AlgebricksException("pre-sorted group-by expects variable references.");
                    }
                    VariableReferenceExpression v = (VariableReferenceExpression) expr;
                    LogicalVariable decor = v.getVariableReference();
                    if (p.first != null) {
                        target.addVariable(p.first);
                    } else {
                        target.addVariable(decor);
                    }
                }

            }
        };
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gByList) {
            if (visitor.transform(p.second)) {
                b = true;
            }
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : decorList) {
            if (visitor.transform(p.second)) {
                b = true;
            }
        }
        return b;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitGroupByOperator(this, arg);
    }

    public static LogicalVariable getDecorVariable(Pair<LogicalVariable, Mutable<ILogicalExpression>> p) {
        if (p.first != null) {
            return p.first;
        } else {
            VariableReferenceExpression e = (VariableReferenceExpression) p.second.getValue();
            return e.getVariableReference();
        }
    }

    public List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> getDecorList() {
        return decorList;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        IVariableTypeEnvironment env = createNestedPlansPropagatingTypeEnvironment(ctx, false);
        ILogicalOperator child = inputs.get(0).getValue();
        IVariableTypeEnvironment env2 = ctx.getOutputTypeEnvironment(child);
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : getGroupByList()) {
            ILogicalExpression expr = p.second.getValue();
            if (p.first != null) {
                env.setVarType(p.first, env2.getType(expr));
                if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    LogicalVariable v1 = ((VariableReferenceExpression) expr).getVariableReference();
                    env.setVarType(v1, env2.getVarType(v1));
                }
            } else {
                VariableReferenceExpression vre = (VariableReferenceExpression) p.second.getValue();
                LogicalVariable v2 = vre.getVariableReference();
                env.setVarType(v2, env2.getVarType(v2));
            }
        }
        for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : getDecorList()) {
            ILogicalExpression expr = p.second.getValue();
            if (p.first != null) {
                env.setVarType(p.first, env2.getType(expr));
            } else {
                VariableReferenceExpression vre = (VariableReferenceExpression) p.second.getValue();
                LogicalVariable v2 = vre.getVariableReference();
                env.setVarType(v2, env2.getVarType(v2));
            }
        }
        return env;
    }

    public boolean isGroupAll() {
        return groupAll;
    }

    public void setGroupAll(boolean groupAll) {
        this.groupAll = groupAll;
        checkGroupAll(groupAll);
    }

    public boolean isGlobal() {
        return global;
    }

    public void setGlobal(boolean global) {
        this.global = global;
    }

    // The groupAll flag can only be set if group by columns are empty.
    private void checkGroupAll(boolean groupAll) {
        if (groupAll && !gByList.isEmpty()) {
            throw new IllegalStateException("Conflicting parameters for GROUP BY: there should be no GROUP BY keys "
                    + "when the GROUP ALL flag is set to true");
        }
    }
}
