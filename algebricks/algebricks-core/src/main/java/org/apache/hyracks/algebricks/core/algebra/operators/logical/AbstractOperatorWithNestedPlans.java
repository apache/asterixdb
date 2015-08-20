/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

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

    public abstract void getUsedVariablesExceptNestedPlans(Collection<LogicalVariable> vars);

    public abstract void getProducedVariablesExceptNestedPlans(Collection<LogicalVariable> vars);

}
