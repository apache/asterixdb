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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class BinaryBooleanOrNullFunctionTypeComputer implements IResultTypeComputer {

    public static final BinaryBooleanOrNullFunctionTypeComputer INSTANCE = new BinaryBooleanOrNullFunctionTypeComputer();

    private BinaryBooleanOrNullFunctionTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg1 = fce.getArguments().get(1).getValue();
        IAType t0, t1;
        try {
            t0 = (IAType) env.getType(arg0);
            t1 = (IAType) env.getType(arg1);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if (t0.getTypeTag() == ATypeTag.NULL && t1.getTypeTag() == ATypeTag.NULL) {
            return BuiltinType.ANULL;
        }
        if (TypeHelper.canBeNull(t0) || TypeHelper.canBeNull(t1)) {
            return AUnionType.createNullableType(BuiltinType.ABOOLEAN);
        }
        return BuiltinType.ABOOLEAN;
    }
}
