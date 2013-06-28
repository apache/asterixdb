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
package edu.uci.ics.asterix.dataflow.data.common;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class AqlExpressionTypeComputer implements IExpressionTypeComputer {

    public static final AqlExpressionTypeComputer INSTANCE = new AqlExpressionTypeComputer();

    private AqlExpressionTypeComputer() {
    }

    @Override
    public Object getType(ILogicalExpression expr, IMetadataProvider<?, ?> metadataProvider,
            IVariableTypeEnvironment env) throws AlgebricksException {
        switch (expr.getExpressionTag()) {
            case CONSTANT: {
                return getTypeForConstant((ConstantExpression) expr, env);
            }
            case FUNCTION_CALL: {
                return getTypeForFunction((AbstractFunctionCallExpression) expr, env, metadataProvider);
            }
            case VARIABLE: {
                return env.getVarType(((VariableReferenceExpression) expr).getVariableReference());
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private IAType getTypeForFunction(AbstractFunctionCallExpression expr, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> mp) throws AlgebricksException {
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
        if (ck != null) {
            List<IAType> unionList = new ArrayList<IAType>();
            unionList.add(BuiltinType.ANULL);
            unionList.add(BuiltinType.ABOOLEAN);
            return new AUnionType(unionList, "OptionalBoolean");
        }
        // Note: only builtin functions, for now.
        IResultTypeComputer rtc = AsterixBuiltinFunctions.getResultTypeComputer(fi);
        if (rtc == null) {
            throw new AlgebricksException("Type computer missing for " + fi);
        }
        return rtc.computeType(expr, env, mp);
    }

    private IAType getTypeForConstant(ConstantExpression expr, IVariableTypeEnvironment env) {
        IAlgebricksConstantValue acv = expr.getValue();
        if (acv.isFalse() || acv.isTrue()) {
            return BuiltinType.ABOOLEAN;
        } else if (acv.isNull()) {
            return BuiltinType.ANULL;
        } else {
            AsterixConstantValue value = (AsterixConstantValue) acv;
            return value.getObject().getType();
        }
    }

}
