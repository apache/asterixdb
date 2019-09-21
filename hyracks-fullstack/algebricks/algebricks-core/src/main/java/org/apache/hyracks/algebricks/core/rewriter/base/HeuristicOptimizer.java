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

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.Level;

public class HeuristicOptimizer {

    private final IOptimizationContext context;
    private final List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> logicalRewrites;
    private final List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> physicalRewrites;
    private final ILogicalPlan plan;

    public static final PhysicalOperatorTag[] hyraxOperatorsBelowWhichJobGenIsDisabled = new PhysicalOperatorTag[] {};

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

        logPlanAt("Plan Before Optimization", Level.TRACE);
        runLogicalOptimizationSets(plan, logicalRewrites);
        computeSchemaBottomUpForPlan(plan);
        runPhysicalOptimizationSets(plan, physicalRewrites);
        logPlanAt("Plan After Optimization", Level.TRACE);
    }

    private void logPlanAt(String name, Level lvl) throws AlgebricksException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isEnabled(lvl)) {
            String planStr = context.getPrettyPrinter().reset().printPlan(plan).toString();
            AlgebricksConfig.ALGEBRICKS_LOGGER.log(lvl, name + ":\n" + LogRedactionUtil.userData(planStr));
        }
    }

    private void runLogicalOptimizationSets(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> optimizationSet)
            throws AlgebricksException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting logical optimizations.\n");
        }
        runOptimizationSets(plan, optimizationSet);
    }

    private void runOptimizationSets(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> optimizationSet)
            throws AlgebricksException {
        for (Pair<AbstractRuleController, List<IAlgebraicRewriteRule>> ruleList : optimizationSet) {
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

    private void runPhysicalOptimizationSets(ILogicalPlan plan,
            List<Pair<AbstractRuleController, List<IAlgebraicRewriteRule>>> optimizationSet)
            throws AlgebricksException {
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Starting physical optimizations.\n");
        }
        runOptimizationSets(plan, optimizationSet);
    }
}
