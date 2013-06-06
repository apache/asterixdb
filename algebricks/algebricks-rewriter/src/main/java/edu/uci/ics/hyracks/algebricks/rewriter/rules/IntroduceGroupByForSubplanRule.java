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
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

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

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SubplanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;

public class IntroduceGroupByForSubplanRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
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
            ILogicalOperator subplanInput = subplan.getInputs().get(0).getValue();
            pkVars = new HashSet<LogicalVariable>();
            VariableUtilities.getLiveVariables(subplanInput, pkVars);
        }
        AlgebricksConfig.ALGEBRICKS_LOGGER.fine("Found FD for introducing group-by: " + pkVars);

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
            case DATASOURCESCAN: {
                DataSourceScanOperator innerScan = (DataSourceScanOperator) right;
                // Select [ $y != null ]
                if (innerScan.getVariables().size() == 1) {
                    testForNull = innerScan.getVariables().get(0);
                }
                break;
            }
        }
        if (testForNull == null) {
            testForNull = context.newVar();
            AssignOperator tmpAsgn = new AssignOperator(testForNull, new MutableObject<ILogicalExpression>(
                    ConstantExpression.TRUE));
            tmpAsgn.getInputs().add(new MutableObject<ILogicalOperator>(rightRef.getValue()));
            rightRef.setValue(tmpAsgn);
            context.computeAndSetTypeEnvironmentForOperator(tmpAsgn);
        }

        IFunctionInfo finfoEq = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.IS_NULL);
        ILogicalExpression isNullTest = new ScalarFunctionCallExpression(finfoEq,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(testForNull)));
        IFunctionInfo finfoNot = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.NOT);
        ScalarFunctionCallExpression nonNullTest = new ScalarFunctionCallExpression(finfoNot,
                new MutableObject<ILogicalExpression>(isNullTest));
        SelectOperator selectNonNull = new SelectOperator(new MutableObject<ILogicalExpression>(nonNullTest));
        GroupByOperator g = new GroupByOperator();
        Mutable<ILogicalOperator> newSubplanRef = new MutableObject<ILogicalOperator>(subplan);
        NestedTupleSourceOperator nts = new NestedTupleSourceOperator(new MutableObject<ILogicalOperator>(g));
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
            g.getDecorList().add(
                    new Pair<LogicalVariable, Mutable<ILogicalExpression>>(null, new MutableObject<ILogicalExpression>(
                            new VariableReferenceExpression(uv))));
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
        Map<LogicalVariable, LogicalVariable> m = new HashMap<LogicalVariable, LogicalVariable>();
        for (LogicalVariable ov : vars) {
            LogicalVariable newVar = context.newVar();
            ILogicalExpression varExpr = new VariableReferenceExpression(newVar);
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
