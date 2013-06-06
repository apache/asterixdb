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
package edu.uci.ics.hyracks.algebricks.rewriter.util;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.FDsAndEquivClassesVisitor;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;

public class PhysicalOptimizationsUtil {

    public static void computeFDsAndEquivalenceClasses(AbstractLogicalOperator op, IOptimizationContext ctx)
            throws AlgebricksException {
        FDsAndEquivClassesVisitor visitor = new FDsAndEquivClassesVisitor();
        Set<ILogicalOperator> visitSet = new HashSet<ILogicalOperator>();
        computeFDsAndEqClassesWithVisitorRec(op, ctx, visitor, visitSet);
    }

    private static void computeFDsAndEqClassesWithVisitorRec(AbstractLogicalOperator op, IOptimizationContext ctx,
            FDsAndEquivClassesVisitor visitor, Set<ILogicalOperator> visitSet) throws AlgebricksException {
        visitSet.add(op);
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            computeFDsAndEqClassesWithVisitorRec((AbstractLogicalOperator) i.getValue(), ctx, visitor, visitSet);
        }
        if (op.hasNestedPlans()) {
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
        if (AlgebricksConfig.DEBUG) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.fine("--> op. type = " + op.getOperatorTag() + "\n"
                    + "    equiv. classes = " + ctx.getEquivalenceClassMap(op) + "\n" + "    FDs = "
                    + ctx.getFDList(op) + "\n");
        }
    }

}
