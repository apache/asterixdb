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

import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IndexedNLJoinExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.EmptyTupleSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class DisjunctivePredicateToJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        SelectOperator select;
        if ((select = asSelectOperator(opRef)) == null) {
            return false;
        }

        AbstractFunctionCallExpression condEx;
        if ((condEx = asFunctionCallExpression(select.getCondition(), AlgebricksBuiltinFunctions.OR)) == null) {
            return false;
        }

        List<Mutable<ILogicalExpression>> args = condEx.getArguments();

        VariableReferenceExpression varEx = null;
        IAType valType = null;
        HashSet<AsterixConstantValue> values = new HashSet<AsterixConstantValue>();

        for (Mutable<ILogicalExpression> arg : args) {
            AbstractFunctionCallExpression fctCall;
            if ((fctCall = asFunctionCallExpression(arg, AlgebricksBuiltinFunctions.EQ)) == null) {
                return false;
            }

            boolean haveConst = false;
            boolean haveVar = false;
            List<Mutable<ILogicalExpression>> fctArgs = fctCall.getArguments();
            for (Mutable<ILogicalExpression> fctArg : fctArgs) {
                final ILogicalExpression argExpr = fctArg.getValue();
                switch (argExpr.getExpressionTag()) {
                    case CONSTANT:
                        haveConst = true;
                        AsterixConstantValue value = (AsterixConstantValue) ((ConstantExpression) argExpr).getValue();
                        if (valType == null) {
                            valType = value.getObject().getType();
                        } else if (!isCompatible(valType, value.getObject().getType())) {
                            return false;
                        }
                        values.add(value);
                        break;
                    case VARIABLE:
                        haveVar = true;
                        final VariableReferenceExpression varArg = (VariableReferenceExpression) argExpr;
                        if (varEx == null) {
                            varEx = varArg;
                        } else if (!varEx.getVariableReference().equals(varArg.getVariableReference())) {
                            return false;
                        }
                        break;
                    default:
                        return false;
                }
            }
            if (!(haveVar && haveConst)) {
                return false;
            }
        }

        AOrderedList list = new AOrderedList(new AOrderedListType(valType, "orderedlist"));
        for (AsterixConstantValue value : values) {
            list.add(value.getObject());
        }

        EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
        context.computeAndSetTypeEnvironmentForOperator(ets);

        ILogicalExpression cExp = new ConstantExpression(new AsterixConstantValue(list));
        Mutable<ILogicalExpression> mutCExp = new MutableObject<ILogicalExpression>(cExp);
        IFunctionInfo scanFctInfo = AsterixBuiltinFunctions
                .getAsterixFunctionInfo(AsterixBuiltinFunctions.SCAN_COLLECTION);
        UnnestingFunctionCallExpression scanExp = new UnnestingFunctionCallExpression(scanFctInfo, mutCExp);
        LogicalVariable scanVar = context.newVar();
        UnnestOperator unn = new UnnestOperator(scanVar, new MutableObject<ILogicalExpression>(scanExp));
        unn.getInputs().add(new MutableObject<ILogicalOperator>(ets));
        context.computeAndSetTypeEnvironmentForOperator(unn);

        IFunctionInfo eqFctInfo = AsterixBuiltinFunctions.getAsterixFunctionInfo(AlgebricksBuiltinFunctions.EQ);
        AbstractFunctionCallExpression eqExp = new ScalarFunctionCallExpression(eqFctInfo);
        eqExp.getArguments().add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(scanVar)));
        eqExp.getArguments().add(new MutableObject<ILogicalExpression>(varEx.cloneExpression()));
        eqExp.getAnnotations().put(IndexedNLJoinExpressionAnnotation.INSTANCE,
                IndexedNLJoinExpressionAnnotation.INSTANCE);

        InnerJoinOperator jOp = new InnerJoinOperator(new MutableObject<ILogicalExpression>(eqExp));
        jOp.getInputs().add(new MutableObject<ILogicalOperator>(unn));
        jOp.getInputs().add(select.getInputs().get(0));

        opRef.setValue(jOp);
        context.computeAndSetTypeEnvironmentForOperator(jOp);

        return true;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    /**
     * This checks the compatibility the types of the constants to ensure that the comparison behaves as expected
     * when joining. Right now this compatibility is defined as type equality, but it could we relaxed.
     * Once type promotion works correctly in all parts of the system, this check should not be needed anymore.
     * (see https://code.google.com/p/asterixdb/issues/detail?id=716)
     * 
     * @param t1
     *            one type
     * @param t2
     *            another type
     * @return true, if types are equal
     */
    private static boolean isCompatible(IAType t1, IAType t2) {
        return t1.equals(t2);
    }

    // some helpers

    private static SelectOperator asSelectOperator(ILogicalOperator op) {
        return op.getOperatorTag() == LogicalOperatorTag.SELECT ? (SelectOperator) op : null;
    }

    private static SelectOperator asSelectOperator(Mutable<ILogicalOperator> op) {
        return asSelectOperator(op.getValue());
    }

    private static AbstractFunctionCallExpression asFunctionCallExpression(ILogicalExpression ex, FunctionIdentifier fi) {
        AbstractFunctionCallExpression fctCall = (ex.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL ? (AbstractFunctionCallExpression) ex
                : null);
        if (fctCall != null && (fi == null || fctCall.getFunctionIdentifier().equals(fi)))
            return fctCall;
        return null;
    }

    private static AbstractFunctionCallExpression asFunctionCallExpression(Mutable<ILogicalExpression> ex,
            FunctionIdentifier fi) {
        return asFunctionCallExpression(ex.getValue(), fi);
    }

}
