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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class IfNanOrInfTypeComputer extends AbstractResultTypeComputer {

    public static final IfNanOrInfTypeComputer INSTANCE = new IfNanOrInfTypeComputer(false);
    public static final IfNanOrInfTypeComputer INSTANCE_SKIP_MISSING = new IfNanOrInfTypeComputer(true);

    private final boolean skipMissing;

    private IfNanOrInfTypeComputer(boolean skipMissing) {
        this.skipMissing = skipMissing;
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        if (strippedInputTypes.length < 2 || strippedInputTypes.length > Short.MAX_VALUE) {
            String functionName = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier().getName();
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, expr.getSourceLocation(),
                    functionName);
        }

        boolean any = false;
        IAType currentType = null;
        for (IAType type : strippedInputTypes) {
            if (currentType != null && !type.equals(currentType)) {
                any = true;
                break;
            }
            currentType = type;
        }
        if (any || currentType == null) {
            return BuiltinType.ANY;
        }

        switch (currentType.getTypeTag()) {
            case MISSING:
                if (skipMissing) {
                    // i.e. all args have been inspected and couldn't find a candidate value, so return null
                    return BuiltinType.ANULL;
                }
            case ANY:
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                return currentType;

            case DOUBLE:
            case FLOAT:
                return AUnionType.createNullableType(currentType, null);

            default:
                return BuiltinType.ANULL;
        }
    }

    @Override
    protected boolean propagateNullAndMissing() {
        return false;
    }
}
