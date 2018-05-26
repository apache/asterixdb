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
package org.apache.asterix.dataflow.data.common;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.ExternalFunctionInfo;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class ExpressionTypeComputer implements IExpressionTypeComputer {

    public static final ExpressionTypeComputer INSTANCE = new ExpressionTypeComputer();

    private ExpressionTypeComputer() {
    }

    @Override
    public Object getType(ILogicalExpression expr, IMetadataProvider<?, ?> metadataProvider,
            IVariableTypeEnvironment env) throws AlgebricksException {
        switch (expr.getExpressionTag()) {
            case CONSTANT:
                return getTypeForConstant((ConstantExpression) expr, env);
            case FUNCTION_CALL:
                return getTypeForFunction((AbstractFunctionCallExpression) expr, env, metadataProvider);
            case VARIABLE:
                try {
                    return env.getVarType(((VariableReferenceExpression) expr).getVariableReference());
                } catch (Exception e) {
                    throw new CompilationException(ErrorCode.COMPILATION_ERROR, expr.getSourceLocation(),
                            "Could not resolve type for " + expr.toString() + ","
                                    + "please check whether the used variable has been defined!",
                            e);
                }
            default:
                throw new IllegalStateException();
        }
    }

    private IAType getTypeForFunction(AbstractFunctionCallExpression expr, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> mp) throws AlgebricksException {
        FunctionIdentifier fi = expr.getFunctionIdentifier();
        // Note: built-in functions + udfs
        IResultTypeComputer rtc;
        FunctionSignature signature = new FunctionSignature(fi);
        if (BuiltinFunctions.isBuiltinCompilerFunction(signature, true)) {
            rtc = BuiltinFunctions.getResultTypeComputer(fi);
        } else {
            rtc = ((ExternalFunctionInfo) expr.getFunctionInfo()).getResultTypeComputer();
        }
        if (rtc == null) {
            throw new AlgebricksException("Type computer missing for " + fi);
        }
        return rtc.computeType(expr, env, mp);
    }

    private IAType getTypeForConstant(ConstantExpression expr, IVariableTypeEnvironment env) {
        IAlgebricksConstantValue acv = expr.getValue();
        if (acv.isFalse() || acv.isTrue()) {
            return BuiltinType.ABOOLEAN;
        } else if (acv.isMissing()) {
            return BuiltinType.AMISSING;
        } else if (acv.isNull()) {
            return BuiltinType.ANULL;
        } else {
            AsterixConstantValue value = (AsterixConstantValue) acv;
            return value.getObject().getType();
        }
    }

}
