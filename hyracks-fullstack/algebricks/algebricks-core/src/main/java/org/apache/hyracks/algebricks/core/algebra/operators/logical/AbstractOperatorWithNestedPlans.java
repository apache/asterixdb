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
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.TypePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.typing.OpRefTypeEnvPointer;
import org.apache.hyracks.algebricks.core.algebra.typing.PropagatingTypeEnvironment;

public abstract class AbstractOperatorWithNestedPlans extends AbstractLogicalOperator {
    protected final List<ILogicalPlan> nestedPlans;

    public AbstractOperatorWithNestedPlans() {
        nestedPlans = new ArrayList<ILogicalPlan>();
    }

    public AbstractOperatorWithNestedPlans(List<ILogicalPlan> nestedPlans) {
        this.nestedPlans = nestedPlans;
    }

    public List<ILogicalPlan> getNestedPlans() {
        return nestedPlans;
    }

    @Override
    public boolean hasNestedPlans() {
        return true;
    }

    public LinkedList<Mutable<ILogicalOperator>> allRootsInReverseOrder() {
        LinkedList<Mutable<ILogicalOperator>> allRoots = new LinkedList<Mutable<ILogicalOperator>>();
        for (ILogicalPlan p : nestedPlans) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                allRoots.addFirst(r);
            }
        }
        return allRoots;
    }

    //
    // @Override
    // public void computeConstraintsAndEquivClasses() {
    // for (ILogicalPlan p : nestedPlans) {
    // for (LogicalOperatorReference r : p.getRoots()) {
    // AbstractLogicalOperator op = (AbstractLogicalOperator) r.getOperator();
    // equivalenceClasses.putAll(op.getEquivalenceClasses());
    // functionalDependencies.addAll(op.getFDs());
    // }
    // }
    // }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getValue().getSchema());
        for (ILogicalPlan p : nestedPlans) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                schema.addAll(r.getValue().getSchema());
            }
        }
    }

    @Override
    public boolean isMap() {
        return false;
    }

    protected PropagatingTypeEnvironment createNestedPlansPropagatingTypeEnvironment(ITypingContext ctx,
            boolean propagateInput) {
        int n = 0;
        for (ILogicalPlan p : nestedPlans) {
            n += p.getRoots().size();
        }

        int i;
        ITypeEnvPointer[] envPointers;
        if (propagateInput) {
            i = inputs.size();
            envPointers = new ITypeEnvPointer[n + i];
            for (int j = 0; j < i; j++) {
                envPointers[j] = new OpRefTypeEnvPointer(inputs.get(j), ctx);
            }
        } else {
            envPointers = new ITypeEnvPointer[n];
            i = 0;
        }
        for (ILogicalPlan p : nestedPlans) {
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                envPointers[i] = new OpRefTypeEnvPointer(r, ctx);
                i++;
            }
        }
        return new PropagatingTypeEnvironment(ctx.getExpressionTypeComputer(), ctx.getMissableTypeComputer(),
                ctx.getMetadataProvider(), TypePropagationPolicy.ALL, envPointers);
    }

    public abstract void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars);

    public abstract void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars);

}
