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
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.asterix.optimizer.base.FuzzyUtils;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class FuzzyEqRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();

        // current operator is INNERJOIN or LEFTOUTERJOIN or SELECT
        Mutable<ILogicalExpression> expRef;
        if (op.getOperatorTag() == LogicalOperatorTag.INNERJOIN
                || op.getOperatorTag() == LogicalOperatorTag.LEFTOUTERJOIN) {
            AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
            expRef = joinOp.getCondition();
        } else if (op.getOperatorTag() == LogicalOperatorTag.SELECT) {
            SelectOperator selectOp = (SelectOperator) op;
            expRef = selectOp.getCondition();
        } else {
            return false;
        }

        AqlMetadataProvider metadataProvider = ((AqlMetadataProvider) context.getMetadataProvider());

        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op);
        if (expandFuzzyEq(expRef, context, env, metadataProvider)) {
            context.computeAndSetTypeEnvironmentForOperator(op);
            return true;
        }
        return false;
    }

    private boolean expandFuzzyEq(Mutable<ILogicalExpression> expRef, IOptimizationContext context,
            IVariableTypeEnvironment env, AqlMetadataProvider metadataProvider) throws AlgebricksException {
        ILogicalExpression exp = expRef.getValue();

        if (exp.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        boolean expanded = false;
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) exp;
        FunctionIdentifier fi = funcExp.getFunctionIdentifier();
        if (fi.equals(AsterixBuiltinFunctions.FUZZY_EQ)) {
            List<Mutable<ILogicalExpression>> inputExps = funcExp.getArguments();

            // TODO: Current hack to be able to optimize selections. 
            // We change the behavior of this rule for the specific cases of const-var, or for edit-distance functions.
            boolean useExprAsIs = false;

            String simFuncName = FuzzyUtils.getSimFunction(metadataProvider);
            ArrayList<Mutable<ILogicalExpression>> similarityArgs = new ArrayList<Mutable<ILogicalExpression>>();
            List<ATypeTag> inputExprTypes = new ArrayList<ATypeTag>();
            for (int i = 0; i < 2; i++) {
                Mutable<ILogicalExpression> inputExpRef = inputExps.get(i);
                ILogicalExpression inputExp = inputExpRef.getValue();

                // get the input type tag
                ATypeTag inputTypeTag = null;
                if (inputExp.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                    VariableReferenceExpression inputVarRef = (VariableReferenceExpression) inputExp;
                    LogicalVariable inputVar = inputVarRef.getVariableReference();
                    IAType t = TypeHelper.getNonOptionalType((IAType) env.getVarType(inputVar));
                    inputExprTypes.add(t.getTypeTag());
                } else if (inputExp.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                    // Hack to make sure that we will add the func call as is, without wrapping a tokenizer around.
                    IAType type = (IAType) context.getExpressionTypeComputer().getType(inputExp, metadataProvider, env);
                    inputTypeTag = type.getTypeTag();
                    // Only auto-tokenize strings.
                    if (inputTypeTag == ATypeTag.STRING) {
                        // Strings will be auto-tokenized.
                        inputTypeTag = ATypeTag.UNORDEREDLIST;
                    } else {
                        useExprAsIs = true;
                    }
                    inputExprTypes.add(inputTypeTag);
                } else if (inputExp.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                    ConstantExpression inputConst = (ConstantExpression) inputExp;
                    AsterixConstantValue constVal = (AsterixConstantValue) inputConst.getValue();
                    inputTypeTag = constVal.getObject().getType().getTypeTag();
                    inputExprTypes.add(inputTypeTag);
                    useExprAsIs = true;
                } else {
                    throw new NotImplementedException();
                }

                if (simFuncName.equals(FuzzyUtils.EDIT_DISTANCE_FUNCTION_NAME)) {
                    useExprAsIs = true;
                }
            }
            // TODO: This second loop is only necessary to implement the hack.
            for (int i = 0; i < inputExprTypes.size(); ++i) {
                Mutable<ILogicalExpression> inputExpRef = inputExps.get(i);
                // TODO: Change Jaccard only to accept sets. We should never have to wrap a tokenizer around.
                // get the tokenizer (if any)
                FunctionIdentifier tokenizer = FuzzyUtils.getTokenizer(inputExprTypes.get(i));
                if (useExprAsIs) {
                    similarityArgs.add(inputExpRef);
                } else if (tokenizer == null) {
                    similarityArgs.add(inputExpRef);
                } else {
                    ArrayList<Mutable<ILogicalExpression>> tokenizerArguments = new ArrayList<Mutable<ILogicalExpression>>();
                    tokenizerArguments.add(inputExpRef);
                    ScalarFunctionCallExpression tokenizerExp = new ScalarFunctionCallExpression(
                            FunctionUtils.getFunctionInfo(tokenizer), tokenizerArguments);
                    similarityArgs.add(new MutableObject<ILogicalExpression>(tokenizerExp));
                }
            }

            FunctionIdentifier simFunctionIdentifier = FuzzyUtils.getFunctionIdentifier(simFuncName);
            ScalarFunctionCallExpression similarityExp = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(simFunctionIdentifier), similarityArgs);
            // Add annotations from the original fuzzy-eq function.
            similarityExp.getAnnotations().putAll(funcExp.getAnnotations());
            ArrayList<Mutable<ILogicalExpression>> cmpArgs = new ArrayList<Mutable<ILogicalExpression>>();
            cmpArgs.add(new MutableObject<ILogicalExpression>(similarityExp));
            IAObject simThreshold = FuzzyUtils.getSimThreshold(metadataProvider, simFuncName);
            cmpArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                    simThreshold))));
            ScalarFunctionCallExpression cmpExpr = FuzzyUtils.getComparisonExpr(simFuncName, cmpArgs);
            expRef.setValue(cmpExpr);
            return true;
        } else if (fi.equals(AlgebricksBuiltinFunctions.AND) || fi.equals(AlgebricksBuiltinFunctions.OR)) {
            for (int i = 0; i < 2; i++) {
                if (expandFuzzyEq(funcExp.getArguments().get(i), context, env, metadataProvider)) {
                    expanded = true;
                }
            }
        }
        return expanded;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }
}
