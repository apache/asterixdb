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

import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class DoubleIfTypeComputer implements IResultTypeComputer {

    public static final DoubleIfTypeComputer INSTANCE = new DoubleIfTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        List<Mutable<ILogicalExpression>> arguments = fce.getArguments();
        if (arguments.size() != 2) {
            String functionName = fce.getFunctionIdentifier().getName();
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, fce.getSourceLocation(),
                    functionName);
        }
        Mutable<ILogicalExpression> firstArg = arguments.get(0);
        IAType firstArgType = (IAType) env.getType(firstArg.getValue());
        if (firstArgType.getTypeTag() == ATypeTag.DOUBLE) {
            return firstArgType;
        } else if (firstArgType.getTypeTag() == ATypeTag.UNION) {
            AUnionType unionType = (AUnionType) firstArgType;
            IAType actualType = unionType.getActualType();
            if (actualType.getTypeTag() == ATypeTag.DOUBLE && unionType.isUnknownableType()) {
                return unionType;
            }
        }

        return BuiltinType.ANY;
    }
}
