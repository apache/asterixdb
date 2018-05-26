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
import java.util.List;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Looks for a select operator, containing a condition:
 * similarity-function GE/GT/LE/LE constant/variable
 * Rewrites the select condition (and possibly the assign expr) with the equivalent similarity-check function.
 */
public class SimilarityCheckRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // Look for select.
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;
        Mutable<ILogicalExpression> condExpr = select.getCondition();

        // Gather assigns below this select.
        List<AssignOperator> assigns = new ArrayList<AssignOperator>();
        AbstractLogicalOperator childOp = (AbstractLogicalOperator) select.getInputs().get(0).getValue();
        // Skip selects.
        while (childOp.getOperatorTag() == LogicalOperatorTag.SELECT) {
            childOp = (AbstractLogicalOperator) childOp.getInputs().get(0).getValue();
        }
        while (childOp.getOperatorTag() == LogicalOperatorTag.ASSIGN) {
            assigns.add((AssignOperator) childOp);
            childOp = (AbstractLogicalOperator) childOp.getInputs().get(0).getValue();
        }
        return replaceSelectConditionExprs(condExpr, assigns, context);
    }

    private boolean replaceSelectConditionExprs(Mutable<ILogicalExpression> expRef, List<AssignOperator> assigns,
            IOptimizationContext context) throws AlgebricksException {
        ILogicalExpression expr = expRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        // Recursively traverse conjuncts.
        // TODO: Ignore disjuncts for now, because some replacements may be invalid.
        // For example, if the result of the similarity function is used somewhere upstream,
        // then we may still need the true similarity value even if the GE/GT/LE/LE comparison returns false.
        if (funcIdent == AlgebricksBuiltinFunctions.AND) {
            boolean found = true;
            for (int i = 0; i < funcExpr.getArguments().size(); ++i) {
                found = found && replaceSelectConditionExprs(funcExpr.getArguments().get(i), assigns, context);
            }
            return found;
        }

        // Look for GE/GT/LE/LT.
        if (funcIdent != AlgebricksBuiltinFunctions.GE && funcIdent != AlgebricksBuiltinFunctions.GT
                && funcIdent != AlgebricksBuiltinFunctions.LE && funcIdent != AlgebricksBuiltinFunctions.LT) {
            return false;
        }

        // One arg should be a function call or a variable, the other a constant.
        AsterixConstantValue constVal = null;
        ILogicalExpression nonConstExpr = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // Normalized GE/GT/LE/LT as if constant was on the right hand side.
        FunctionIdentifier normFuncIdent = null;
        // One of the args must be a constant.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) arg1;
            constVal = (AsterixConstantValue) constExpr.getValue();
            nonConstExpr = arg2;
            // Get func ident as if swapping lhs and rhs.
            normFuncIdent = getLhsAndRhsSwappedFuncIdent(funcIdent);
        } else if (arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) arg2;
            constVal = (AsterixConstantValue) constExpr.getValue();
            nonConstExpr = arg1;
            // Constant is already on rhs, so nothing to be done for normalizedFuncIdent.
            normFuncIdent = funcIdent;
        } else {
            return false;
        }

        // The other arg is a function call. We can directly replace the select condition with an equivalent similarity check expression.
        if (nonConstExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return replaceWithFunctionCallArg(expRef, normFuncIdent, constVal,
                    (AbstractFunctionCallExpression) nonConstExpr);
        }
        // The other arg ist a variable. We may have to introduce an assign operator that assigns the result of a similarity-check function to a variable.
        if (nonConstExpr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            return replaceWithVariableArg(expRef, normFuncIdent, constVal, (VariableReferenceExpression) nonConstExpr,
                    assigns, context);
        }
        return false;
    }

    private boolean replaceWithVariableArg(Mutable<ILogicalExpression> expRef, FunctionIdentifier normFuncIdent,
            AsterixConstantValue constVal, VariableReferenceExpression varRefExpr, List<AssignOperator> assigns,
            IOptimizationContext context) throws AlgebricksException {

        // Find variable in assigns to determine its originating function.
        LogicalVariable var = varRefExpr.getVariableReference();
        Mutable<ILogicalExpression> simFuncExprRef = null;
        ScalarFunctionCallExpression simCheckFuncExpr = null;
        AssignOperator matchingAssign = null;
        for (int i = 0; i < assigns.size(); i++) {
            AssignOperator assign = assigns.get(i);
            for (int j = 0; j < assign.getVariables().size(); j++) {
                // Check if variables match.
                if (var != assign.getVariables().get(j)) {
                    continue;
                }
                // Check if corresponding expr is a function call.
                if (assign.getExpressions().get(j).getValue()
                        .getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                    continue;
                }
                simFuncExprRef = assign.getExpressions().get(j);
                // Analyze function expression and get equivalent similarity check function.
                simCheckFuncExpr = getSimilarityCheckExpr(normFuncIdent, constVal,
                        (AbstractFunctionCallExpression) simFuncExprRef.getValue());
                matchingAssign = assign;
                break;
            }
            if (simCheckFuncExpr != null) {
                break;
            }
        }

        // Only non-null if we found that varRefExpr refers to an optimizable similarity function call.
        if (simCheckFuncExpr != null) {
            SourceLocation sourceLoc = simCheckFuncExpr.getSourceLocation();
            // Create a new assign under matchingAssign which assigns the result of our similarity-check function to a variable.
            LogicalVariable newVar = context.newVar();
            AssignOperator newAssign =
                    new AssignOperator(newVar, new MutableObject<ILogicalExpression>(simCheckFuncExpr));
            newAssign.setSourceLocation(sourceLoc);
            // Hook up inputs.
            newAssign.getInputs()
                    .add(new MutableObject<ILogicalOperator>(matchingAssign.getInputs().get(0).getValue()));
            matchingAssign.getInputs().get(0).setValue(newAssign);

            // Replace select condition with a get-item on newVarFromExpression.
            List<Mutable<ILogicalExpression>> selectGetItemArgs = new ArrayList<Mutable<ILogicalExpression>>();
            // First arg is a variable reference expr on newVarFromExpression.
            VariableReferenceExpression newVarRef1 = new VariableReferenceExpression(newVar);
            newVarRef1.setSourceLocation(sourceLoc);
            selectGetItemArgs.add(new MutableObject<ILogicalExpression>(newVarRef1));
            // Second arg is the item index to be accessed, here 0.
            selectGetItemArgs.add(new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(0)))));
            ScalarFunctionCallExpression selectGetItemExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.GET_ITEM), selectGetItemArgs);
            selectGetItemExpr.setSourceLocation(sourceLoc);
            // Replace the old similarity function call with the new getItemExpr.
            expRef.setValue(selectGetItemExpr);

            // Replace expr corresponding to original variable in the original assign with a get-item on
            // newVarFromExpression.
            List<Mutable<ILogicalExpression>> assignGetItemArgs = new ArrayList<Mutable<ILogicalExpression>>();
            // First arg is a variable reference expr on newVarFromExpression.
            VariableReferenceExpression newVarRef2 = new VariableReferenceExpression(newVar);
            newVarRef2.setSourceLocation(sourceLoc);
            assignGetItemArgs.add(new MutableObject<ILogicalExpression>(newVarRef2));
            // Second arg is the item index to be accessed, here 1.
            assignGetItemArgs.add(new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(1)))));
            ScalarFunctionCallExpression assignGetItemExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.GET_ITEM), assignGetItemArgs);
            assignGetItemExpr.setSourceLocation(sourceLoc);
            // Replace the original assign expr with the get-item expr.
            simFuncExprRef.setValue(assignGetItemExpr);

            context.computeAndSetTypeEnvironmentForOperator(newAssign);
            context.computeAndSetTypeEnvironmentForOperator(matchingAssign);

            return true;
        }

        return false;
    }

    private boolean replaceWithFunctionCallArg(Mutable<ILogicalExpression> expRef, FunctionIdentifier normFuncIdent,
            AsterixConstantValue constVal, AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        // Analyze func expr to see if it is an optimizable similarity function.
        ScalarFunctionCallExpression simCheckFuncExpr = getSimilarityCheckExpr(normFuncIdent, constVal, funcExpr);

        // Replace the expr in the select condition.
        if (simCheckFuncExpr != null) {
            // Get item 0 from var.
            List<Mutable<ILogicalExpression>> getItemArgs = new ArrayList<Mutable<ILogicalExpression>>();
            // First arg is the similarity-check function call.
            getItemArgs.add(new MutableObject<ILogicalExpression>(simCheckFuncExpr));
            // Second arg is the item index to be accessed.
            getItemArgs.add(new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(new AInt32(0)))));
            ScalarFunctionCallExpression getItemExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.GET_ITEM), getItemArgs);
            getItemExpr.setSourceLocation(simCheckFuncExpr.getSourceLocation());
            // Replace the old similarity function call with the new getItemExpr.
            expRef.setValue(getItemExpr);
            return true;
        }

        return false;
    }

    private ScalarFunctionCallExpression getSimilarityCheckExpr(FunctionIdentifier normFuncIdent,
            AsterixConstantValue constVal, AbstractFunctionCallExpression funcExpr) throws AlgebricksException {
        // Remember args from original similarity function to add them to the similarity-check function later.
        ArrayList<Mutable<ILogicalExpression>> similarityArgs = null;
        ScalarFunctionCallExpression simCheckFuncExpr = null;
        // Look for jaccard function call, and GE or GT.
        if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.SIMILARITY_JACCARD) {
            IAObject jaccThresh;
            if (normFuncIdent == AlgebricksBuiltinFunctions.GE) {
                if (constVal.getObject() instanceof AFloat) {
                    jaccThresh = constVal.getObject();
                } else {
                    jaccThresh = new AFloat((float) ((ADouble) constVal.getObject()).getDoubleValue());
                }
            } else if (normFuncIdent == AlgebricksBuiltinFunctions.GT) {
                float threshVal = 0.0f;
                if (constVal.getObject() instanceof AFloat) {
                    threshVal = ((AFloat) constVal.getObject()).getFloatValue();
                } else {
                    threshVal = (float) ((ADouble) constVal.getObject()).getDoubleValue();
                }
                float f = threshVal + Float.MIN_VALUE;
                if (f > 1.0f)
                    f = 1.0f;
                jaccThresh = new AFloat(f);
            } else {
                return null;
            }
            similarityArgs = new ArrayList<Mutable<ILogicalExpression>>();
            similarityArgs.addAll(funcExpr.getArguments());
            similarityArgs.add(new MutableObject<ILogicalExpression>(
                    new ConstantExpression(new AsterixConstantValue(jaccThresh))));
            simCheckFuncExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.SIMILARITY_JACCARD_CHECK), similarityArgs);
            simCheckFuncExpr.setSourceLocation(funcExpr.getSourceLocation());
        }

        // Look for edit-distance function call, and LE or LT.
        if (funcExpr.getFunctionIdentifier() == BuiltinFunctions.EDIT_DISTANCE) {
            AInt32 aInt = new AInt32(0);
            try {
                aInt = (AInt32) ATypeHierarchy.convertNumericTypeObject(constVal.getObject(), ATypeTag.INTEGER);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }

            AInt32 edThresh;
            if (normFuncIdent == AlgebricksBuiltinFunctions.LE) {
                edThresh = aInt;
            } else if (normFuncIdent == AlgebricksBuiltinFunctions.LT) {
                int ed = aInt.getIntegerValue() - 1;
                if (ed < 0)
                    ed = 0;
                edThresh = new AInt32(ed);
            } else {
                return null;
            }
            similarityArgs = new ArrayList<Mutable<ILogicalExpression>>();
            similarityArgs.addAll(funcExpr.getArguments());
            similarityArgs.add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(edThresh))));
            simCheckFuncExpr = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(BuiltinFunctions.EDIT_DISTANCE_CHECK), similarityArgs);
            simCheckFuncExpr.setSourceLocation(funcExpr.getSourceLocation());
        }
        // Preserve all annotations.
        if (simCheckFuncExpr != null) {
            simCheckFuncExpr.getAnnotations().putAll(funcExpr.getAnnotations());
        }
        return simCheckFuncExpr;
    }

    private FunctionIdentifier getLhsAndRhsSwappedFuncIdent(FunctionIdentifier oldFuncIdent) {
        if (oldFuncIdent == AlgebricksBuiltinFunctions.GE) {
            return AlgebricksBuiltinFunctions.LE;
        }
        if (oldFuncIdent == AlgebricksBuiltinFunctions.GT) {
            return AlgebricksBuiltinFunctions.LT;
        }
        if (oldFuncIdent == AlgebricksBuiltinFunctions.LE) {
            return AlgebricksBuiltinFunctions.GE;
        }
        if (oldFuncIdent == AlgebricksBuiltinFunctions.LT) {
            return AlgebricksBuiltinFunctions.GT;
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }
}
