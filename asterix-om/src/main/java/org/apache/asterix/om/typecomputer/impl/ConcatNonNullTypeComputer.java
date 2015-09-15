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

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * The type computer for concat-not-null.
 * Note that this function is only used for the if-then-else clause.
 *
 * @author yingyib
 */
public class ConcatNonNullTypeComputer implements IResultTypeComputer {

    public static final ConcatNonNullTypeComputer INSTANCE = new ConcatNonNullTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        if (f.getArguments().size() < 1) {
            return BuiltinType.ANULL;
        }

        TypeCompatibilityChecker tcc = new TypeCompatibilityChecker();
        for (int i = 0; i < f.getArguments().size(); i++) {
            ILogicalExpression arg = f.getArguments().get(i).getValue();
            IAType type = (IAType) env.getType(arg);
            tcc.addPossibleType(type);
        }

        IAType result = tcc.getCompatibleType();
        if (result == null) {
            throw new AlgebricksException("The two branches of the if-else clause should return the same type.");
        }
        return result;
    }
}
