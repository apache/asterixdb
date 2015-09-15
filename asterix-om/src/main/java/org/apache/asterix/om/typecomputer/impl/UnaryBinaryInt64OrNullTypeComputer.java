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
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class UnaryBinaryInt64OrNullTypeComputer implements IResultTypeComputer {
    public static final UnaryBinaryInt64OrNullTypeComputer INSTANCE = new UnaryBinaryInt64OrNullTypeComputer();

    private UnaryBinaryInt64OrNullTypeComputer() {

    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().size() != 1) {
            throw new AlgebricksException("Wrong Argument Number.");
        }
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        IAType t0;
        t0 = (IAType) env.getType(arg0);
        if (t0.getTypeTag() != ATypeTag.NULL
                && t0.getTypeTag() != ATypeTag.BINARY
                && (t0.getTypeTag() == ATypeTag.UNION && !((AUnionType) t0).getUnionList()
                        .contains(BuiltinType.ABINARY))) {
            throw new NotImplementedException("Expects Binary Type.");
        }

        if (t0.getTypeTag() == ATypeTag.NULL) {
            return BuiltinType.ANULL;
        }

        if (t0.getTypeTag() == ATypeTag.BINARY || t0.getTypeTag().equals(ATypeTag.UNION)) {
            return AUnionType.createNullableType(BuiltinType.AINT64, "binary-length-Result");
        }
        throw new AlgebricksException("Cannot compute type");
    }
}
