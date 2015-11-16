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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.asterix.algebra.base.AsterixOperatorAnnotations;
import org.apache.asterix.lang.aql.util.FunctionUtils;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ByNameToByIndexFieldAccessRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }
        AssignOperator assign = (AssignOperator) op;
        if (assign.getExpressions().get(0).getValue().getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        List<Mutable<ILogicalExpression>> expressions = assign.getExpressions();
        boolean changed = false;
        for (int i = 0; i < expressions.size(); i++) {
            AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expressions.get(i).getValue();
            if (fce.getFunctionIdentifier() != AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME) {
                continue;
            }
            IVariableTypeEnvironment env = context.getOutputTypeEnvironment(op);

            ILogicalExpression a0 = fce.getArguments().get(0).getValue();
            if (a0.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                LogicalVariable var1 = context.newVar();
                ArrayList<LogicalVariable> varArray = new ArrayList<LogicalVariable>(1);
                varArray.add(var1);
                ArrayList<Mutable<ILogicalExpression>> exprArray = new ArrayList<Mutable<ILogicalExpression>>(1);
                exprArray.add(new MutableObject<ILogicalExpression>(a0));
                AssignOperator assignVar = new AssignOperator(varArray, exprArray);
                fce.getArguments().get(0).setValue(new VariableReferenceExpression(var1));
                assignVar.getInputs().add(new MutableObject<ILogicalOperator>(assign.getInputs().get(0).getValue()));
                assign.getInputs().get(0).setValue(assignVar);
                context.computeAndSetTypeEnvironmentForOperator(assignVar);
                context.computeAndSetTypeEnvironmentForOperator(assign);
                //access by name was not replaced to access by index, but the plan was altered, hence changed is true
                changed = true;
            }

            IAType t = (IAType) env.getType(fce.getArguments().get(0).getValue());
            try {
                switch (t.getTypeTag()) {
                    case ANY: {
                        return false || changed;
                    }
                    case RECORD: {
                        ARecordType recType = (ARecordType) t;
                        ILogicalExpression fai = createFieldAccessByIndex(recType, fce);
                        if (fai == null) {
                            return false || changed;
                        }
                        expressions.get(i).setValue(fai);
                        changed = true;
                        break;
                    }
                    case UNION: {
                        AUnionType unionT = (AUnionType) t;
                        if (unionT.isNullableType()) {
                            IAType t2 = unionT.getNullableType();
                            if (t2.getTypeTag() == ATypeTag.RECORD) {
                                ARecordType recType = (ARecordType) t2;
                                ILogicalExpression fai = createFieldAccessByIndex(recType, fce);
                                if (fai == null) {
                                    return false || changed;
                                }
                                expressions.get(i).setValue(fai);
                                changed = true;
                                break;
                            }
                        }
                        throw new NotImplementedException("Union " + unionT);
                    }
                    default: {
                        throw new AlgebricksException("Cannot call field-access on data of type " + t);
                    }
                }
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }
        assign.removeAnnotation(AsterixOperatorAnnotations.PUSHED_FIELD_ACCESS);
        return changed;
    }

    @SuppressWarnings("unchecked")
    private static ILogicalExpression createFieldAccessByIndex(ARecordType recType, AbstractFunctionCallExpression fce)
            throws IOException {
        String s = getStringSecondArgument(fce);
        if (s == null) {
            return null;
        }
        int k = recType.findFieldPosition(s);
        if (k < 0) {
            return null;
        }
        return new ScalarFunctionCallExpression(
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX), fce.getArguments().get(0),
                new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(k)))));
    }

    private static String getStringSecondArgument(AbstractFunctionCallExpression expr) {
        ILogicalExpression e2 = expr.getArguments().get(1).getValue();
        if (e2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        ConstantExpression c = (ConstantExpression) e2;
        if (!(c.getValue() instanceof AsterixConstantValue)) {
            return null;
        }
        IAObject v = ((AsterixConstantValue) c.getValue()).getObject();
        if (v.getType().getTypeTag() != ATypeTag.STRING) {
            return null;
        }
        return ((AString) v).getStringValue();
    }
}
