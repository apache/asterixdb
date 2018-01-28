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
package org.apache.hyracks.algebricks.rewriter.util;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.FDsAndEquivClassesVisitor;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class PhysicalOptimizationsUtil {

    public static void computeFDsAndEquivalenceClasses(ILogicalOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        FDsAndEquivClassesVisitor visitor = new FDsAndEquivClassesVisitor();
        visitOperatorAndItsDescendants(op, visitor, ctx);
    }

    public static <R> void visitOperatorAndItsDescendants(ILogicalOperator op,
            ILogicalOperatorVisitor<R, IOptimizationContext> visitor, IOptimizationContext ctx)
            throws AlgebricksException {
        Set<ILogicalOperator> visitSet = new HashSet<ILogicalOperator>();
        computeFDsAndEqClassesWithVisitorRec(op, ctx, visitor, visitSet);
    }

    private static <R> void computeFDsAndEqClassesWithVisitorRec(ILogicalOperator op, IOptimizationContext ctx,
            ILogicalOperatorVisitor<R, IOptimizationContext> visitor, Set<ILogicalOperator> visitSet)
            throws AlgebricksException {
        visitSet.add(op);
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            computeFDsAndEqClassesWithVisitorRec((AbstractLogicalOperator) i.getValue(), ctx, visitor, visitSet);
        }
        AbstractLogicalOperator aop = (AbstractLogicalOperator) op;
        if (aop.hasNestedPlans()) {
            for (ILogicalPlan p : ((AbstractOperatorWithNestedPlans) op).getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    AbstractLogicalOperator rootOp = (AbstractLogicalOperator) r.getValue();
                    computeFDsAndEqClassesWithVisitorRec(rootOp, ctx, visitor, visitSet);
                }
            }
        }
        if (op.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
            NestedTupleSourceOperator nts = (NestedTupleSourceOperator) op;
            ILogicalOperator source = nts.getDataSourceReference().getValue().getInputs().get(0).getValue();
            if (!visitSet.contains(source)) {
                computeFDsAndEqClassesWithVisitorRec((AbstractLogicalOperator) source, ctx, visitor, visitSet);
            }
        }
        op.accept(visitor, ctx);
    }

}
