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
package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.base.AsterixOperatorAnnotations;
import edu.uci.ics.asterix.common.exceptions.AsterixRuntimeException;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.optimizer.base.AnalysisUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class LoadRecordFieldsRule implements IAlgebraicRewriteRule {

    private ExtractFieldLoadExpressionVisitor exprVisitor = new ExtractFieldLoadExpressionVisitor();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }

        if (op1.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            AssignOperator a1 = (AssignOperator) op1;
            ILogicalExpression expr = getFirstExpr(a1);
            if (AnalysisUtil.isAccessToFieldRecord(expr)) {
                boolean res = findAndEliminateRedundantFieldAccess(a1);
                context.addToDontApplySet(this, op1);
                return res;
            }
        }
        exprVisitor.setTopOp(op1);
        exprVisitor.setContext(context);
        boolean res = op1.acceptExpressionTransform(exprVisitor);
        if (!res) {
            context.addToDontApplySet(this, op1);
        }
        if (res && op1.getOperatorTag() == LogicalOperatorTag.SELECT) {
            // checking if we can annotate a Selection as using just one field
            // access
            SelectOperator sigma = (SelectOperator) op1;
            LinkedList<LogicalVariable> vars = new LinkedList<LogicalVariable>();
            VariableUtilities.getUsedVariables(sigma, vars);
            if (vars.size() == 1) {
                // we can annotate Selection
                AssignOperator assign1 = (AssignOperator) op1.getInputs().get(0).getValue();
                AbstractLogicalExpression expr1 = (AbstractLogicalExpression) getFirstExpr(assign1);
                if (expr1.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr1;
                    // f should be a call to a field/data access kind of
                    // function
                    sigma.getAnnotations().put(AsterixOperatorAnnotations.FIELD_ACCESS, f.getArguments().get(0));
                }
            }
        }

        // TODO: avoid having to recompute type env. here
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        return res;
    }

    private static boolean pushFieldLoads(Mutable<ILogicalExpression> exprRef, AbstractLogicalOperator topOp,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = exprRef.getValue();
        if (expr == null) {
            return false;
        }
        switch (expr.getExpressionTag()) {
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
                FunctionIdentifier fi = f.getFunctionIdentifier();
                if (AlgebricksBuiltinFunctions.isComparisonFunction(fi)) {
                    boolean b1 = pushFieldLoads(f.getArguments().get(0), topOp, context);
                    boolean b2 = pushFieldLoads(f.getArguments().get(1), topOp, context);
                    return b1 || b2;
                }
                if (fi.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                    if (AnalysisUtil.numberOfVarsInExpr(f) == 0) {
                        return false;
                    }
                    // create an assign
                    LogicalVariable v = context.newVar();
                    AssignOperator a2 = new AssignOperator(v, new MutableObject<ILogicalExpression>(f));
                    pushFieldAssign(a2, topOp, context);
                    context.computeAndSetTypeEnvironmentForOperator(a2);
                    ILogicalExpression arg = f.getArguments().get(0).getValue();
                    if (arg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                        VariableReferenceExpression ref = (VariableReferenceExpression) arg;
                        LogicalVariable var = ref.getVariableReference();
                        List<LogicalVariable> keys = context.findPrimaryKey(var);
                        if (keys != null) {
                            List<LogicalVariable> tail = new ArrayList<LogicalVariable>();
                            tail.add(v);
                            FunctionalDependency pk = new FunctionalDependency(keys, tail);
                            context.addPrimaryKey(pk);
                        }
                    }
                    exprRef.setValue(new VariableReferenceExpression(v));
                    return true;
                } else {
                    boolean pushed = false;
                    for (Mutable<ILogicalExpression> argRef : f.getArguments()) {
                        if (pushFieldLoads(argRef, topOp, context)) {
                            pushed = true;
                        }
                    }
                    return pushed;
                }
            }
            case CONSTANT:
            case VARIABLE: {
                return false;
            }
            default: {
                assert false;
                throw new IllegalArgumentException();
            }
        }
    }

    private static void pushFieldAssign(AssignOperator a2, AbstractLogicalOperator topOp, IOptimizationContext context)
            throws AlgebricksException {
        if (topOp.getInputs().size() == 1 && !topOp.hasNestedPlans()) {
            Mutable<ILogicalOperator> topChild = topOp.getInputs().get(0);
            // plugAccessAboveOp(a2, topChild, context);
            List<Mutable<ILogicalOperator>> a2InptList = a2.getInputs();
            a2InptList.clear();
            a2InptList.add(topChild);
            // and link it as child in the op. tree
            topOp.getInputs().set(0, new MutableObject<ILogicalOperator>(a2));
            findAndEliminateRedundantFieldAccess(a2);
        } else { // e.g., a join
            LinkedList<LogicalVariable> usedInAccess = new LinkedList<LogicalVariable>();
            VariableUtilities.getUsedVariables(a2, usedInAccess);

            LinkedList<LogicalVariable> produced2 = new LinkedList<LogicalVariable>();
            VariableUtilities.getProducedVariables(topOp, produced2);

            if (OperatorPropertiesUtil.disjoint(produced2, usedInAccess)) {
                for (Mutable<ILogicalOperator> inp : topOp.getInputs()) {
                    HashSet<LogicalVariable> v2 = new HashSet<LogicalVariable>();
                    VariableUtilities.getLiveVariables(inp.getValue(), v2);
                    if (!OperatorPropertiesUtil.disjoint(usedInAccess, v2)) {
                        pushAccessAboveOpRef(a2, inp, context);
                        return;
                    }
                }
                if (topOp.hasNestedPlans()) {
                    AbstractOperatorWithNestedPlans nestedOp = (AbstractOperatorWithNestedPlans) topOp;
                    for (ILogicalPlan plan : nestedOp.getNestedPlans()) {
                        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
                            HashSet<LogicalVariable> v2 = new HashSet<LogicalVariable>();
                            VariableUtilities.getLiveVariables(root.getValue(), v2);
                            if (!OperatorPropertiesUtil.disjoint(usedInAccess, v2)) {
                                pushAccessAboveOpRef(a2, root, context);
                                return;
                            }
                        }
                    }
                }
                throw new AsterixRuntimeException("Field access " + getFirstExpr(a2)
                        + " does not correspond to any input of operator " + topOp);
            }
        }
    }

    /**
     * 
     * Pushes one field-access assignment above toPushThroughChildRef
     * 
     * @param toPush
     * @param toPushThroughChildRef
     */
    private static void pushAccessAboveOpRef(AssignOperator toPush, Mutable<ILogicalOperator> toPushThroughChildRef,
            IOptimizationContext context) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> tpInpList = toPush.getInputs();
        tpInpList.clear();
        tpInpList.add(new MutableObject<ILogicalOperator>(toPushThroughChildRef.getValue()));
        toPushThroughChildRef.setValue(toPush);
        findAndEliminateRedundantFieldAccess(toPush);
    }

    /**
     * 
     * Rewrite
     * 
     * assign $x := field-access($y, "field")
     * 
     * assign $y := record-constructor { "field": Expr, ... }
     * 
     * into
     * 
     * assign $x := Expr
     * 
     * assign $y := record-constructor { "field": Expr, ... }
     * 
     * @param toPush
     */
    private static boolean findAndEliminateRedundantFieldAccess(AssignOperator assign) throws AlgebricksException {
        ILogicalExpression expr = getFirstExpr(assign);
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
        ILogicalExpression arg0 = f.getArguments().get(0).getValue();
        if (arg0.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }
        VariableReferenceExpression vre = (VariableReferenceExpression) arg0;
        LogicalVariable recordVar = vre.getVariableReference();
        ILogicalExpression arg1 = f.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        ConstantExpression ce = (ConstantExpression) arg1;
        if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
            String fldName = ((AString) ((AsterixConstantValue) ce.getValue()).getObject()).getStringValue();
            ILogicalExpression fldExpr = findFieldExpression(assign, recordVar, fldName);

            if (fldExpr != null) {
                // check the liveness of the new expression
                List<LogicalVariable> usedVariables = new ArrayList<LogicalVariable>();
                fldExpr.getUsedVariables(usedVariables);
                List<LogicalVariable> liveInputVars = new ArrayList<LogicalVariable>();
                VariableUtilities.getLiveVariables(assign, liveInputVars);
                usedVariables.removeAll(liveInputVars);
                if (usedVariables.size() == 0) {
                    assign.getExpressions().get(0).setValue(fldExpr);
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else if (f.getFunctionIdentifier().equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            // int fldIdx = ((IntegerLiteral) ce.getValue()).getValue();
            // TODO
            return false;
        } else {
            throw new IllegalStateException();
        }
    }

    private static ILogicalExpression findFieldExpression(AbstractLogicalOperator op, LogicalVariable recordVar,
            String fldName) {
        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            AbstractLogicalOperator opChild = (AbstractLogicalOperator) child.getValue();
            if (opChild.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator op2 = (AssignOperator) opChild;
                int i = 0;
                for (LogicalVariable var : op2.getVariables()) {
                    if (var == recordVar) {
                        AbstractLogicalExpression constr = (AbstractLogicalExpression) op2.getExpressions().get(i)
                                .getValue();
                        if (constr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                            return null;
                        }
                        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) constr;
                        if (!fce.getFunctionIdentifier().equals(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)
                                && !fce.getFunctionIdentifier().equals(
                                        AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
                            return null;
                        }
                        Iterator<Mutable<ILogicalExpression>> fldIter = fce.getArguments().iterator();
                        while (fldIter.hasNext()) {
                            ILogicalExpression fldExpr = fldIter.next().getValue();
                            if (fldExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                                ConstantExpression ce = (ConstantExpression) fldExpr;
                                String f2 = ((AString) ((AsterixConstantValue) ce.getValue()).getObject())
                                        .getStringValue();
                                if (fldName.equals(f2)) {
                                    return fldIter.next().getValue();
                                }
                            }
                            fldIter.next();
                        }
                        return null;
                    }
                    i++;
                }
            } else if (opChild.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                NestedTupleSourceOperator nts = (NestedTupleSourceOperator) opChild;
                AbstractLogicalOperator opBelowNestedPlan = (AbstractLogicalOperator) nts.getDataSourceReference()
                        .getValue().getInputs().get(0).getValue();
                ILogicalExpression expr1 = findFieldExpression(opBelowNestedPlan, recordVar, fldName);
                if (expr1 != null) {
                    return expr1;
                }
            }
            ILogicalExpression expr2 = findFieldExpression(opChild, recordVar, fldName);
            if (expr2 != null) {
                return expr2;
            }
        }
        return null;
    }

    private final class ExtractFieldLoadExpressionVisitor implements ILogicalExpressionReferenceTransform {

        private AbstractLogicalOperator topOp;
        private IOptimizationContext context;

        public void setTopOp(AbstractLogicalOperator topOp) {
            this.topOp = topOp;
        }

        public void setContext(IOptimizationContext context) {
            this.context = context;
        }

        @Override
        public boolean transform(Mutable<ILogicalExpression> exprRef) throws AlgebricksException {
            return pushFieldLoads(exprRef, topOp, context);
        }

    }

    private static ILogicalExpression getFirstExpr(AssignOperator assign) {
        return assign.getExpressions().get(0).getValue();
    }

}
