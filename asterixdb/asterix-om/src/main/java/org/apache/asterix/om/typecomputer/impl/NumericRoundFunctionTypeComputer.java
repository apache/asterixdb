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
/*
 * Numeric Unary Functions like abs
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

/**
 * Type computer for round function. This type computer receives 1 or 2 arguments. The 1st argument is the value to
 * round, and the 2nd argument (optional) is the digit to round to. The behavior of the type computer is as follows:
 *
 * - For integer types, the return type is int64.
 * - For float type, the return type is float.
 * - For double type, the return type is double.
 * - For any type, the return type is any.
 * - For all other types, the return type is null.
 */

public class NumericRoundFunctionTypeComputer extends AbstractResultTypeComputer {
    public static final NumericRoundFunctionTypeComputer INSTANCE = new NumericRoundFunctionTypeComputer();

    private NumericRoundFunctionTypeComputer() {
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType returnType;
        ATypeTag firstArgType = strippedInputTypes[0].getTypeTag();
        ATypeTag secondArgType = strippedInputTypes.length > 1 ? strippedInputTypes[1].getTypeTag() : null;

        switch (firstArgType) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                returnType = BuiltinType.AINT64;
                break;
            case FLOAT:
            case DOUBLE:
            case ANY:
                returnType = strippedInputTypes[0];
                break;
            default:
                return BuiltinType.ANULL;
        }

        // No second argument
        if (secondArgType == null) {
            return returnType;
        }

        switch (secondArgType) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case ANY:
                return returnType;
            case FLOAT:
            case DOUBLE:
                return AUnionType.createNullableType(returnType);
            default:
                return BuiltinType.ANULL;
        }
    }
}
