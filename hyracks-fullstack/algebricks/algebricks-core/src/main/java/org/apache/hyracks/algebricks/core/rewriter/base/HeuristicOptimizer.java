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
package org.apache.hyracks.algebricks.core.rewriter.base;

import java.util.List;
import java.util.logging.Level;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.PlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;

public class HeuristicOptimizer {

    public static PhysicalOperatorTag[] hyracksOperators = new PhysicalOperatorTag[] {
            PhysicalOperatorTag.DATASOURCE_SCAN, PhysicalOperatorTag.BTREE_SEARCH,
            PhysicalOperatorTag.EXTERNAL_GROUP_BY, PhysicalOperatorTag.HASH_GROUP_BY, PhysicalOperatorTag.HDFS_READER,
            PhysicalOperatorTag.HYBRID_HASH_JOIN, PhysicalOperatorTag.IN_MEMORY_HASH_JOIN,
            PhysicalOperatorTag.NESTED_LOOP, PhysicalOperatorTag.PRE_SORTED_DISTINCT_BY,
            PhysicalOperatorTag.PRE_CLUSTERED_GROUP_BY, PhysicalOperatorTag.REPLICATE, PhysicalOperatorTag.STABLE_SORT,
            PhysicalOperatorTag.UNION_ALL };
    public static PhysicalOperatorTag[] hyraxOperatorsBelowWhichJobGenIsDisabled = new PhysicalOperatorTag[] {};

    public static boolean isHyracksOp(PhysicalOperatorTag opTag) {
        for (PhysicalOperatorTag t : hyracksOperators) {
            if (t == opTag) {
                return true;
            }
        }
        return false;
    }

    private final IOptimizationContext context;
    private final List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites;
    private final List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites;
    private final ILogicalPlan plan;

    public HeuristicOptimizer(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites,
            IOptimizationContext context) {
        this.plan = plan;
        this.context = context;
        this.logicalRewrites = logicalRewrites;
        this.physicalRewrites = physicalRewrites;
    }

    public void optimize() throws AlgebricksException {
        if (plan == null) {
            return;
        }
        if (AlgebricksConfig.DEBUG) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Starting logical optimizations.\n");
        }

        logPlanAt("Logical Plan", Level.FINE);
        runOptimizationSets(plan, logicalRewrites);
        computeSchemaBottomUpForPlan(plan);
        runPhysicalOptimizations(plan, physicalRewrites);
        logPlanAt("Optimized Plan", Level.FINE);
    }

    private void logPlanAt(String name, Level lvl) throws AlgebricksException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isLoggable(lvl)) {
            final LogicalOperatorPrettyPrintVisitor pvisitor = context.getPrettyPrintVisitor();
            pvisitor.reset(new AlgebricksAppendable());
            PlanPrettyPrinter.printPlan(plan, pvisitor, 0);
            AlgebricksConfig.ALGEBRICKS_LOGGER.info(name + ":\n" + pvisitor.get().toString());
        }
    }

    private void runOptimizationSets(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> optimSet) throws AlgebricksException {
        for (Pair<AbstractRuleController, List<IAlgebraicRewriteRule>> ruleList : optimSet) {
            for (Mutable<ILogicalOperator> r : plan.getRoots()) {
                ruleList.first.setContext(context);
                ruleList.first.rewriteWithRuleCollection(r, ruleList.second);
            }
        }
    }

    private static void computeSchemaBottomUpForPlan(ILogicalPlan p) throws AlgebricksException {
        for (Mutable<ILogicalOperator> r : p.getRoots()) {
            computeSchemaBottomUpForOp((AbstractLogicalOperator) r.getValue());
        }
    }

    private static void computeSchemaBottomUpForOp(AbstractLogicalOperator op) throws AlgebricksException {
        for (Mutable<ILogicalOperator> i : op.getInputs()) {
            computeSchemaBottomUpForOp((AbstractLogicalOperator) i.getValue());
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans a = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : a.getNestedPlans()) {
                computeSchemaBottomUpForPlan(p);
            }
        }
        op.recomputeSchema();
    }

    private void runPhysicalOptimizations(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites)
            throws AlgebricksException {
        if (AlgebricksConfig.DEBUG) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Starting physical optimizations.\n");
        }
        // PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(plan);
        runOptimizationSets(plan, physicalRewrites);
    }

}
