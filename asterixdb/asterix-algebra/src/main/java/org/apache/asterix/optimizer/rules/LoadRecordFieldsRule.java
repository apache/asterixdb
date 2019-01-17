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
package org.apache.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.algebra.base.OperatorAnnotation;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.optimizer.base.AnalysisUtil;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

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
                boolean res = findAndEliminateRedundantFieldAccess(a1, context);
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
                    sigma.getAnnotations().put(OperatorAnnotation.FIELD_ACCESS, f.getArguments().get(0));
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
                if (fi.equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                    if (AnalysisUtil.numberOfVarsInExpr(f) == 0) {
                        return false;
                    }
                    // create an assign
                    LogicalVariable v = context.newVar();
                    AssignOperator a2 = new AssignOperator(v, new MutableObject<ILogicalExpression>(f));
                    a2.setSourceLocation(expr.getSourceLocation());
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
                    VariableReferenceExpression varRef = new VariableReferenceExpression(v);
                    varRef.setSourceLocation(expr.getSourceLocation());
                    exprRef.setValue(varRef);
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
            topOp.getInputs().set(0, new MutableObject<>(a2));
            findAndEliminateRedundantFieldAccess(a2, context);
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
                throw new CompilationException(ErrorCode.COMPILATION_ERROR, a2.getSourceLocation(),
                        "Field access " + getFirstExpr(a2) + " does not correspond to any input");
            }
        }
    }

    /**
     * Pushes one field-access assignment above toPushThroughChildRef
     *
     * @param toPush
     * @param toPushThroughChildRef
     */
    private static void pushAccessAboveOpRef(AssignOperator toPush, Mutable<ILogicalOperator> toPushThroughChildRef,
            IOptimizationContext context) throws AlgebricksException {
        List<Mutable<ILogicalOperator>> tpInpList = toPush.getInputs();
        tpInpList.clear();
        tpInpList.add(new MutableObject<>(toPushThroughChildRef.getValue()));
        toPushThroughChildRef.setValue(toPush);
        findAndEliminateRedundantFieldAccess(toPush, context);
    }

    /**
     * Rewrite
     * assign $x := field-access($y, "field")
     * assign $y := record-constructor { "field": Expr, ... }
     * into
     * assign $x := Expr
     * assign $y := record-constructor { "field": Expr, ... }
     */
    private static boolean findAndEliminateRedundantFieldAccess(AssignOperator assign, IOptimizationContext context)
            throws AlgebricksException {
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
        IVariableTypeEnvironment typeEnvironment = context.getOutputTypeEnvironment(assign);
        ConstantExpression ce = (ConstantExpression) arg1;
        ILogicalExpression fldExpr;
        if (f.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
            String fldName = ((AString) ((AsterixConstantValue) ce.getValue()).getObject()).getStringValue();
            fldExpr = findFieldExpression(assign, recordVar, fldName, typeEnvironment,
                    (name, expression, env) -> findFieldByNameFromRecordConstructor(name, expression));
        } else if (f.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_BY_INDEX)) {
            Integer fldIdx = ((AInt32) ((AsterixConstantValue) ce.getValue()).getObject()).getIntegerValue();
            fldExpr = findFieldExpression(assign, recordVar, fldIdx, typeEnvironment,
                    LoadRecordFieldsRule::findFieldByIndexFromRecordConstructor);
        } else if (f.getFunctionIdentifier().equals(BuiltinFunctions.FIELD_ACCESS_NESTED)) {
            return false;
        } else {
            throw new IllegalStateException();
        }

        if (fldExpr == null) {
            return false;
        }
        // check the liveness of the new expression
        List<LogicalVariable> usedVariables = new ArrayList<>();
        fldExpr.getUsedVariables(usedVariables);
        List<LogicalVariable> liveInputVars = new ArrayList<>();
        VariableUtilities.getLiveVariables(assign, liveInputVars);
        usedVariables.removeAll(liveInputVars);
        if (usedVariables.isEmpty()) {
            assign.getExpressions().get(0).setValue(fldExpr);
            return true;
        } else {
            return false;
        }
    }

    @FunctionalInterface
    private interface FieldResolver {
        ILogicalExpression resolve(Object accessKey, AbstractFunctionCallExpression funcExpr,
                IVariableTypeEnvironment typeEnvironment) throws AlgebricksException;
    }

    // Finds a field expression.
    private static ILogicalExpression findFieldExpression(AbstractLogicalOperator op, LogicalVariable recordVar,
            Object accessKey, IVariableTypeEnvironment typeEnvironment, FieldResolver resolver)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> child : op.getInputs()) {
            AbstractLogicalOperator opChild = (AbstractLogicalOperator) child.getValue();
            if (opChild.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
                AssignOperator op2 = (AssignOperator) opChild;
                int i = op2.getVariables().indexOf(recordVar);
                if (i >= 0) {
                    AbstractLogicalExpression constr =
                            (AbstractLogicalExpression) op2.getExpressions().get(i).getValue();
                    return resolveFieldExpression(constr, accessKey, typeEnvironment, resolver);
                }
            } else if (opChild.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE) {
                NestedTupleSourceOperator nts = (NestedTupleSourceOperator) opChild;
                AbstractLogicalOperator opWithNestedPlan =
                        (AbstractLogicalOperator) nts.getDataSourceReference().getValue();
                ILogicalExpression expr1 =
                        findFieldExpression(opWithNestedPlan, recordVar, accessKey, typeEnvironment, resolver);
                if (expr1 != null) {
                    return expr1;
                }
            }
            ILogicalExpression expr2 = findFieldExpression(opChild, recordVar, accessKey, typeEnvironment, resolver);
            if (expr2 != null) {
                return expr2;
            }
        }
        return null;
    }

    // Resolves field expression from an access key and a field resolver.
    private static ILogicalExpression resolveFieldExpression(AbstractLogicalExpression constr, Object accessKey,
            IVariableTypeEnvironment typeEnvironment, FieldResolver resolver) throws AlgebricksException {
        if (constr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) constr;
        if (!fce.getFunctionIdentifier().equals(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR)
                && !fce.getFunctionIdentifier().equals(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR)) {
            return null;
        }
        return resolver.resolve(accessKey, fce, typeEnvironment);
    }

    // Resolves field expression by name-based access.
    private static ILogicalExpression findFieldByNameFromRecordConstructor(Object fldName,
            AbstractFunctionCallExpression fce) {
        Iterator<Mutable<ILogicalExpression>> fldIter = fce.getArguments().iterator();
        while (fldIter.hasNext()) {
            ILogicalExpression fldExpr = fldIter.next().getValue();
            if (fldName.equals(ConstantExpressionUtil.getStringConstant(fldExpr))) {
                return fldIter.next().getValue();
            }
            fldIter.next();
        }
        return null;
    }

    // Resolves field expression by index-based access.
    private static ILogicalExpression findFieldByIndexFromRecordConstructor(Object index,
            AbstractFunctionCallExpression fce, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException {
        Integer fieldIndex = (Integer) index;
        ARecordType recordType = (ARecordType) typeEnvironment.getType(fce);
        String[] closedFieldNames = recordType.getFieldNames();
        return closedFieldNames.length > fieldIndex
                ? findFieldByNameFromRecordConstructor(closedFieldNames[fieldIndex], fce) : null;
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
