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
package org.apache.hyracks.algebricks.rewriter.rules.subplan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.config.AlgebricksConfig;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * The rule searches for SUBPLAN operator with a optional PROJECT operator and
 * an AGGREGATE followed by a join operator.
 *
 * <pre>
 * Before
 *
 *   plan__parent
 *   SUBPLAN {
 *     PROJECT?
 *     AGGREGATE
 *     plan__nested_A
 *     INNER_JOIN | LEFT_OUTER_JOIN ($condition, $left, $right)
 *       plan__nested_B
 *   }
 *   plan__child
 *
 *   where $condition does not equal a constant true.
 *
 * After (This is a general application of the rule, specifics may vary based on the query plan.)
 *
 *   plan__parent
 *   GROUP_BY {
 *     PROJECT?
 *     AGGREGATE
 *     plan__nested_A
 *     SELECT( algebricks:not( is_null( $right ) ) )
 *     NESTED_TUPLE_SOURCE
 *   }
 *   SUBPLAN {
 *     INNER_JOIN | LEFT_OUTER_JOIN ($condition, $left, $right)
 *       plan__nested_B
 *   }
 *   plan__child
 * </pre>
 *
 * @author prestonc
 */

public class IntroduceGroupByForSubplanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op0 = (AbstractLogicalOperator) opRef.getValue();
        if (op0.getOperatorTag() != LogicalOperatorTag.SUBPLAN) {
            return false;
        }
        SubplanOperator subplan = (SubplanOperator) op0;

        Iterator<ILogicalPlan> plansIter = subplan.getNestedPlans().iterator();
        ILogicalPlan p = null;
        while (plansIter.hasNext()) {
            p = plansIter.next();
        }
        if (p == null) {
            return false;
        }
        if (p.getRoots().size() != 1) {
            return false;
        }
        Mutable<ILogicalOperator> subplanRoot = p.getRoots().get(0);
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) subplanRoot.getValue();

        Mutable<ILogicalOperator> botRef = subplanRoot;
        AbstractLogicalOperator op2;
        // Project is optional
        if (op1.getOperatorTag() != LogicalOperatorTag.PROJECT) {
            op2 = op1;
        } else {
            ProjectOperator project = (ProjectOperator) op1;
            botRef = project.getInputs().get(0);
            op2 = (AbstractLogicalOperator) botRef.getValue();
        }
        if (op2.getOperatorTag() != LogicalOperatorTag.AGGREGATE) {
            return false;
        }
        AggregateOperator aggregate = (AggregateOperator) op2;

        Set<LogicalVariable> free = new HashSet<LogicalVariable>();
        VariableUtilities.getUsedVariables(aggregate, free);

        Mutable<ILogicalOperator> op3Ref = aggregate.getInputs().get(0);
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) op3Ref.getValue();

        while (op3.getInputs().size() == 1) {
            Set<LogicalVariable> prod = new HashSet<LogicalVariable>();
            VariableUtilities.getProducedVariables(op3, prod);
            free.removeAll(prod);
            VariableUtilities.getUsedVariables(op3, free);
            botRef = op3Ref;
            op3Ref = op3.getInputs().get(0);
            op3 = (AbstractLogicalOperator) op3Ref.getValue();
        }

        if (op3.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && op3.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) op3;
        if (join.getCondition().getValue() == ConstantExpression.TRUE) {
            return false;
        }
        VariableUtilities.getUsedVariables(join, free);

        AbstractLogicalOperator b0 = (AbstractLogicalOperator) join.getInputs().get(0).getValue();
        // see if there's an NTS at the end of the pipeline
        NestedTupleSourceOperator outerNts = getNts(b0);
        if (outerNts == null) {
            AbstractLogicalOperator b1 = (AbstractLogicalOperator) join.getInputs().get(1).getValue();
            outerNts = getNts(b1);
            if (outerNts == null) {
                return false;
            }
        }

        Set<LogicalVariable> pkVars = computeGbyVars(outerNts, free, context);
        if (pkVars == null || pkVars.size() < 1) {
            // there is no non-trivial primary key, group-by keys are all live variables
            // that were produced by descendant or self
            ILogicalOperator subplanInput = subplan.getInputs().get(0).getValue();
            pkVars = new HashSet<LogicalVariable>();
            //get live variables
            VariableUtilities.getLiveVariables(subplanInput, pkVars);

            //get produced variables
            Set<LogicalVariable> producedVars = new HashSet<LogicalVariable>();
            VariableUtilities.getProducedVariablesInDescendantsAndSelf(subplanInput, producedVars);

            //retain the intersection
            pkVars.retainAll(producedVars);
        }
        if (AlgebricksConfig.ALGEBRICKS_LOGGER.isTraceEnabled()) {
            AlgebricksConfig.ALGEBRICKS_LOGGER.trace("Found FD for introducing group-by: " + pkVars);
        }

        Mutable<ILogicalOperator> rightRef = join.getInputs().get(1);
        LogicalVariable testForNull = null;
        AbstractLogicalOperator right = (AbstractLogicalOperator) rightRef.getValue();
        switch (right.getOperatorTag()) {
            case UNNEST: {
                UnnestOperator innerUnnest = (UnnestOperator) right;
                // Select [ $y != null ]
                testForNull = innerUnnest.getVariable();
                break;
            }
            case RUNNINGAGGREGATE: {
                ILogicalOperator inputToRunningAggregate = right.getInputs().get(0).getValue();
                Set<LogicalVariable> producedVars = new ListSet<LogicalVariable>();
                VariableUtilities.getProducedVariables(inputToRunningAggregate, producedVars);
                if (!producedVars.isEmpty()) {
                    // Select [ $y != null ]
                    testForNull = producedVars.iterator().next();
                }
                break;
            }
            case DATASOURCESCAN: {
                DataSourceScanOperator innerScan = (DataSourceScanOperator) right;
                // Select [ $y != null ]
                if (innerScan.getVariables().size() == 1) {
                    testForNull = innerScan.getVariables().get(0);
                }
                break;
            }
            default:
                break;
        }

        SourceLocation sourceLoc = subplan.getSourceLocation();

        if (testForNull == null) {
            testForNull = context.newVar();
            AssignOperator tmpAsgn =
                    new AssignOperator(testForNull, new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
            tmpAsgn.setSourceLocation(sourceLoc);
            tmpAsgn.getInputs().add(new MutableObject<ILogicalOperator>(rightRef.getValue()));
            rightRef.setValue(tmpAsgn);
            context.computeAndSetTypeEnvironmentForOperator(tmpAsgn);
        }

        IFunctionInfo finfoEq = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.IS_MISSING);
        ScalarFunctionCallExpression isNullTest = new ScalarFunctionCallExpression(finfoEq,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(testForNull)));
        isNullTest.setSourceLocation(sourceLoc);
        IFunctionInfo finfoNot = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.NOT);
        ScalarFunctionCallExpression nonNullTest =
                new ScalarFunctionCallExpression(finfoNot, new MutableObject<ILogicalExpression>(isNullTest));
        nonNullTest.setSourceLocation(sourceLoc);
        SelectOperator selectNonNull =
                new SelectOperator(new MutableObject<ILogicalExpression>(nonNullTest), false, null);
        selectNonNull.setSourceLocation(sourceLoc);
        GroupByOperator g = new GroupByOperator();
        g.setSourceLocation(sourceLoc);
        Mutable<ILogicalOperator> newSubplanRef = new MutableObject<ILogicalOperator>(subplan);
        NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(g));
        nts.setSourceLocation(sourceLoc);
        opRef.setValue(g);
        selectNonNull.getInputs().add(new MutableObject<ILogicalOperator>(nts));

        List<Mutable<ILogicalOperator>> prodInpList = botRef.getValue().getInputs();
        prodInpList.clear();
        prodInpList.add(new MutableObject<ILogicalOperator>(selectNonNull));

        ILogicalPlan gPlan = new ALogicalPlanImpl(new MutableObject<ILogicalOperator>(subplanRoot.getValue()));
        g.getNestedPlans().add(gPlan);
        subplanRoot.setValue(op3Ref.getValue());
        g.getInputs().add(newSubplanRef);

        HashSet<LogicalVariable> underVars = new HashSet<LogicalVariable>();
        VariableUtilities.getLiveVariables(subplan.getInputs().get(0).getValue(), underVars);
        underVars.removeAll(pkVars);
        Map<LogicalVariable, LogicalVariable> mappedVars = buildVarExprList(pkVars, context, g, g.getGroupByList());
        context.updatePrimaryKeys(mappedVars);
        for (LogicalVariable uv : underVars) {
            g.getDecorList().add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(null,
                    new MutableObject<ILogicalExpression>(new VariableReferenceExpression(uv))));
        }
        OperatorPropertiesUtil.typeOpRec(subplanRoot, context);
        OperatorPropertiesUtil.typeOpRec(gPlan.getRoots().get(0), context);
        context.computeAndSetTypeEnvironmentForOperator(g);
        return true;
    }

    private NestedTupleSourceOperator getNts(AbstractLogicalOperator op) {
        AbstractLogicalOperator alo = op;
        do {
            if (alo.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                return (NestedTupleSourceOperator) alo;
            }
            if (alo.getInputs().size() != 1) {
                return null;
            }
            alo = (AbstractLogicalOperator) alo.getInputs().get(0).getValue();
        } while (true);
    }

    protected Set<LogicalVariable> computeGbyVars(AbstractLogicalOperator op, Set<LogicalVariable> freeVars,
            IOptimizationContext context) throws AlgebricksException {
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);
        List<FunctionalDependency> fdList = context.getFDList(op);
        if (fdList == null) {
            return null;
        }
        // check if any of the FDs is a key
        List<LogicalVariable> all = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(op, all);
        all.retainAll(freeVars);
        for (FunctionalDependency fd : fdList) {
            if (fd.getTail().containsAll(all)) {
                return new HashSet<LogicalVariable>(fd.getHead());
            }
        }
        return null;
    }

    private Map<LogicalVariable, LogicalVariable> buildVarExprList(Collection<LogicalVariable> vars,
            IOptimizationContext context, GroupByOperator g,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> outVeList) throws AlgebricksException {
        SourceLocation sourceLoc = g.getSourceLocation();
        Map<LogicalVariable, LogicalVariable> m = new HashMap<LogicalVariable, LogicalVariable>();
        for (LogicalVariable ov : vars) {
            LogicalVariable newVar = context.newVar();
            ILogicalExpression varExpr = new VariableReferenceExpression(newVar);
            ((VariableReferenceExpression) varExpr).setSourceLocation(sourceLoc);
            outVeList.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(ov,
                    new MutableObject<ILogicalExpression>(varExpr)));
            for (ILogicalPlan p : g.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    OperatorManipulationUtil.substituteVarRec((AbstractLogicalOperator) r.getValue(), ov, newVar, true,
                            context);
                }
            }
            AbstractLogicalOperator opUnder = (AbstractLogicalOperator) g.getInputs().get(0).getValue();
            OperatorManipulationUtil.substituteVarRec(opUnder, ov, newVar, true, context);
            m.put(ov, newVar);
        }
        return m;
    }
}
