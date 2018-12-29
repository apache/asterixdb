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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class SubplanOperator extends AbstractOperatorWithNestedPlans {

    public SubplanOperator() {
        super();
    }

    public SubplanOperator(List<ILogicalPlan> plans) {
        super(plans);
    }

    public SubplanOperator(ILogicalOperator planRoot) {
        ArrayList<Mutable<ILogicalOperator>> roots = new ArrayList<Mutable<ILogicalOperator>>(1);
        roots.add(new MutableObject<ILogicalOperator>(planRoot));
        nestedPlans.add(new ALogicalPlanImpl(roots));
    }

    public void setRootOp(Mutable<ILogicalOperator> opRef) {
        ILogicalPlan p = new ALogicalPlanImpl(opRef);
        nestedPlans.add(p);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) {
        // do nothing
        return false;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.SUBPLAN;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ADDNEWVARIABLES;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitSubplanOperator(this, arg);
    }

    @Override
    public void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        // do nothing
    }

    @Override
    public void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars) {
        // do nothing
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createNestedPlansPropagatingTypeEnvironment(ctx, true);
    }
}
