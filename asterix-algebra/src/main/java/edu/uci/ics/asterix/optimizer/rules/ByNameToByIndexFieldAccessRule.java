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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.base.AsterixOperatorAnnotations;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class ByNameToByIndexFieldAccessRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
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
            }

            IAType t = (IAType) env.getType(fce.getArguments().get(0).getValue());
            try {
                switch (t.getTypeTag()) {
                    case ANY: {
                        return false;
                    }
                    case RECORD: {
                        ARecordType recType = (ARecordType) t;
                        ILogicalExpression fai = createFieldAccessByIndex(recType, fce);
                        if (fai == null) {
                            return false;
                        }
                        expressions.get(i).setValue(fai);
                        changed = true;
                        break;
                    }
                    case UNION: {
                        AUnionType unionT = (AUnionType) t;
                        if (unionT.isNullableType()) {
                            IAType t2 = unionT.getUnionList().get(1);
                            if (t2.getTypeTag() == ATypeTag.RECORD) {
                                ARecordType recType = (ARecordType) t2;
                                ILogicalExpression fai = createFieldAccessByIndex(recType, fce);
                                if (fai == null) {
                                    return false;
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
                FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX),
                fce.getArguments().get(0), new MutableObject<ILogicalExpression>(new ConstantExpression(
                        new AsterixConstantValue(new AInt32(k)))));
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
