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
package org.apache.asterix.om.typecomputer.base;

import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * This abstract class takes care of the handling of optional types.
 * If a subclass follows the MISSING-in-MISSING-out and NULL-in-NULL-out semantics,
 * then it only needs to think of non-optional types and this abstract class
 * will strip the input types and wrap the output type.
 */
public abstract class AbstractResultTypeComputer implements IResultTypeComputer {

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression functionCallExpression = (AbstractFunctionCallExpression) expression;
        String funcName = functionCallExpression.getFunctionIdentifier().getName();
        return TypeComputeUtils.resolveResultType(expression, env, (index, type) -> checkArgType(funcName, index, type),
                this::getResultType, true);
    }

    /**
     * Checks whether an input type violates the requirement.
     *
     * @param funcName
     *            the function name.
     * @param argIndex,
     *            the index of the argument to consider.
     * @param type,
     *            the type of the input argument.
     * @throws AlgebricksException
     */
    protected void checkArgType(String funcName, int argIndex, IAType type) throws AlgebricksException {

    }

    /**
     * Returns the result type without considering optional types.
     *
     * @param expr
     *            the expression under consideration.
     * @param strippedInputTypes,
     *            the stripped input types.
     * @return the result type without considering optional types.
     * @throws AlgebricksException
     */
    protected abstract IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes)
            throws AlgebricksException;
}
