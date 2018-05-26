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
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.optimizer.base.FuzzyUtils;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

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

        MetadataProvider metadataProvider = ((MetadataProvider) context.getMetadataProvider());

        IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op);
        if (expandFuzzyEq(expRef, context, env, metadataProvider)) {
            context.computeAndSetTypeEnvironmentForOperator(op);
            return true;
        }
        return false;
    }

    private boolean expandFuzzyEq(Mutable<ILogicalExpression> expRef, IOptimizationContext context,
            IVariableTypeEnvironment env, MetadataProvider metadataProvider) throws AlgebricksException {
        ILogicalExpression exp = expRef.getValue();

        if (exp.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        boolean expanded = false;
        AbstractFunctionCallExpression funcExp = (AbstractFunctionCallExpression) exp;
        FunctionIdentifier fi = funcExp.getFunctionIdentifier();
        if (fi.equals(BuiltinFunctions.FUZZY_EQ)) {
            SourceLocation sourceLoc = funcExp.getSourceLocation();
            List<Mutable<ILogicalExpression>> inputExps = funcExp.getArguments();

            String simFuncName = FuzzyUtils.getSimFunction(metadataProvider);
            ArrayList<Mutable<ILogicalExpression>> similarityArgs = new ArrayList<Mutable<ILogicalExpression>>();
            for (int i = 0; i < inputExps.size(); ++i) {
                Mutable<ILogicalExpression> inputExpRef = inputExps.get(i);
                similarityArgs.add(inputExpRef);
            }

            FunctionIdentifier simFunctionIdentifier = FuzzyUtils.getFunctionIdentifier(simFuncName);
            ScalarFunctionCallExpression similarityExp = new ScalarFunctionCallExpression(
                    FunctionUtil.getFunctionInfo(simFunctionIdentifier), similarityArgs);
            similarityExp.setSourceLocation(sourceLoc);
            // Add annotations from the original fuzzy-eq function.
            similarityExp.getAnnotations().putAll(funcExp.getAnnotations());
            ArrayList<Mutable<ILogicalExpression>> cmpArgs = new ArrayList<Mutable<ILogicalExpression>>();
            cmpArgs.add(new MutableObject<ILogicalExpression>(similarityExp));
            IAObject simThreshold = FuzzyUtils.getSimThreshold(metadataProvider, simFuncName);
            ConstantExpression simThresholdExpr = new ConstantExpression(new AsterixConstantValue(simThreshold));
            simThresholdExpr.setSourceLocation(sourceLoc);
            cmpArgs.add(new MutableObject<ILogicalExpression>(simThresholdExpr));
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
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }
}
