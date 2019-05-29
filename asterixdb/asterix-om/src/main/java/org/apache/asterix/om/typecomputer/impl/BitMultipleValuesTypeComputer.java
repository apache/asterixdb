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

/**
 * This type computer expects one or more arguments. The return type is int32 or int64 depending on the function used.
 *
 * The behavior of this type computer is as follows:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then int32 or int64 is returned.
 * - If the argument is float or double, then int32, int64 or null is returned, depending on the argument value.
 * - If the argument is any other type, then null is returned.
 */

public class BitMultipleValuesTypeComputer extends AbstractResultTypeComputer {

    public static final BitMultipleValuesTypeComputer INSTANCE_INT32 =
            new BitMultipleValuesTypeComputer(1, BuiltinType.AINT32);
    public static final BitMultipleValuesTypeComputer INSTANCE_INT64 =
            new BitMultipleValuesTypeComputer(1, BuiltinType.AINT64);

    private BitMultipleValuesTypeComputer(int minArgs, IAType returnType) {
        this.minArgs = minArgs;
        this.returnType = returnType;
    }

    private final int minArgs;
    private final IAType returnType;

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {

        // Ensure minimum arguments are passed
        if (strippedInputTypes.length < minArgs) {
            String functionName = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier().getName();
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, expr.getSourceLocation(),
                    functionName);
        }

        // Result can be nullable in case of invalid arguments like double value of 4.5 (4.0 is acceptable, 4.5 is not)
        boolean isReturnNullable = false;

        // Check that all arguments are of valid type, otherwise a null is returned
        for (IAType type : strippedInputTypes) {
            switch (type.getTypeTag()) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case ANY:
                    continue;
                case FLOAT:
                case DOUBLE:
                    isReturnNullable = true;
                    continue;
                default:
                    return BuiltinType.ANULL;
            }
        }

        return isReturnNullable ? AUnionType.createNullableType(returnType) : returnType;
    }
}
