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
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NumericAddSubMulTypeDescriptor implements IResultTypeComputer {

    private static final String errMsg = "Arithmetic operations are only implemented for AINT32 and ADOUBLE.";

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg2 = fce.getArguments().get(1).getValue();
        IAType t1;
        IAType t2;
        try {
            t1 = (IAType) env.getType(arg1);
            t2 = (IAType) env.getType(arg2);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if (t1 == null || t2 == null) {
            return null;
        }
        switch (t1.getTypeTag()) {
            case INT32: {
                switch (t2.getTypeTag()) {
                    case INT32: {
                        return BuiltinType.AINT32;
                    }
                    case DOUBLE: {
                        return BuiltinType.ADOUBLE;
                    }
                    default: {
                        throw new NotImplementedException(errMsg);
                    }
                }
            }
            case DOUBLE: {
                switch (t2.getTypeTag()) {
                    case INT32:
                    case DOUBLE: {
                        return BuiltinType.ADOUBLE;
                    }
                    default: {
                        throw new NotImplementedException(errMsg);
                    }
                }
            }
            default: {
                throw new NotImplementedException(errMsg);
            }
        }
    }

}
