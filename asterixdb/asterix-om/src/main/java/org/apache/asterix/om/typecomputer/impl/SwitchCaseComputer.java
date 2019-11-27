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
package org.apache.asterix.om.typecomputer.impl;

import static org.apache.asterix.om.types.BuiltinType.ANULL;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.common.TypeResolverUtil;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class SwitchCaseComputer implements IResultTypeComputer {

    public static final IResultTypeComputer INSTANCE = new SwitchCaseComputer();

    private SwitchCaseComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        String funcName = fce.getFunctionIdentifier().getName();

        int argSize = fce.getArguments().size();
        if (argSize < 3) {
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_PARAMETER_NUMBER, fce.getSourceLocation(),
                    funcName, argSize);
        }
        List<IAType> types = new ArrayList<>();
        // Collects different branches' return types.
        // The last return expression is from the ELSE branch and it is optional.
        for (int argIndex = 2; argIndex < argSize; argIndex += (argIndex + 2 == argSize) ? 1 : 2) {
            IAType type = (IAType) env.getType(fce.getArguments().get(argIndex).getValue());
            types.add(type);
        }
        // TODO(ali): investigate if needed for CASE. assumption seems to be that CASE is always rewritten with default
        if (addDefaultNull(argSize)) {
            types.add(ANULL);
        }
        return TypeResolverUtil.resolve(types);
    }

    private boolean addDefaultNull(int argSize) {
        // null is the default value for odd arg size (e.g. fun(cond_exp, exp1, res1))
        return argSize % 2 != 0;
    }
}
