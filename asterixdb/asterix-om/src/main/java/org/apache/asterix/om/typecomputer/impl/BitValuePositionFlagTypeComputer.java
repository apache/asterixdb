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

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

/**
 * This type computer expects 2 or 3 arguments, first argument representing the value, second argument representing
 * the position(s), and third argument (optional) representing a boolean flag. Each instance contains the type it is
 * returning, this is referred to as returnType in the below details.
 *
 * The behavior of this type computer is as follows:
 * For the first argument:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then returnType is returned.
 * - If the argument is float or double, then returnType or null is returned, depending on the argument value.
 * - If the argument is any other type, then null is returned.
 *
 * For the second argument:
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is int64, then returnType is returned.
 * - If the argument is float or double, then returnType or null is returned, depending on the argument value.
 * - If the position argument is an array:
 *      - If the array item is missing or null, then missing or null is returned, respectively.
 *      - If the array item is int64, then returnType is returned.
 *      - If the array item is float or double, then returnType or null is returned, depending on the argument value.
 *      - If the array item is any other type, then null is returned.
 * - If the argument is any other type, then null is returned.
 *
 * For the third argument (if provided):
 * - If the argument is missing or null, then missing or null is returned, respectively.
 * - If the argument is of type boolean, then returnType is returned.
 * - If the argument is any other type, then null is returned.
 */

public class BitValuePositionFlagTypeComputer extends AbstractResultTypeComputer {

    public static final BitValuePositionFlagTypeComputer INSTANCE_SET_CLEAR =
            new BitValuePositionFlagTypeComputer(2, true, BuiltinType.AINT64);
    public static final BitValuePositionFlagTypeComputer INSTANCE_SHIFT_WITH_FLAG =
            new BitValuePositionFlagTypeComputer(2, false, BuiltinType.AINT64);
    public static final BitValuePositionFlagTypeComputer INSTANCE_SHIFT_WITHOUT_FLAG =
            new BitValuePositionFlagTypeComputer(3, false, BuiltinType.AINT64);
    public static final BitValuePositionFlagTypeComputer INSTANCE_TEST_WITH_FLAG =
            new BitValuePositionFlagTypeComputer(2, true, BuiltinType.ABOOLEAN);
    public static final BitValuePositionFlagTypeComputer INSTANCE_TEST_WITHOUT_FLAG =
            new BitValuePositionFlagTypeComputer(3, true, BuiltinType.ABOOLEAN);

    private BitValuePositionFlagTypeComputer(int numberOfArguments, boolean secondArgCanBeArray, IAType returnType) {
        this.numberOfArguments = numberOfArguments;
        this.secondArgCanBeArray = secondArgCanBeArray;
        this.returnType = returnType;
    }

    private final int numberOfArguments;
    private final boolean secondArgCanBeArray;
    private final IAType returnType;

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {

        // Result can be nullable in case of invalid arguments like double value of 4.5 (4.0 is acceptable, 4.5 is not)
        boolean isReturnNullable = false;

        IAType firstArgument = strippedInputTypes[0];
        IAType secondArgument = strippedInputTypes[1];

        // First argument needs to be numeric
        switch (firstArgument.getTypeTag()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case ANY:
                break;
            case FLOAT:
            case DOUBLE:
                isReturnNullable = true;
                break;
            default:
                return BuiltinType.ANULL;
        }

        // Second argument needs to be numeric
        switch (secondArgument.getTypeTag()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case ANY:
                break;
            case FLOAT:
            case DOUBLE:
                isReturnNullable = true;
                break;
            case ARRAY:
                if (!secondArgCanBeArray) {
                    return BuiltinType.ANULL;
                }
                // In case of array, ensure the validity of array item type
                AOrderedListType listType = (AOrderedListType) secondArgument;
                IAType itemType = listType.getItemType();

                switch (itemType.getTypeTag()) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                        break;
                    case FLOAT:
                    case DOUBLE:
                    case ANY:
                        isReturnNullable = true;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            default:
                return BuiltinType.ANULL;
        }

        // Third argument needs to be a boolean
        // The strippedInputTypes.length >= 3 is for some tests to work properly, can be removed when the test is fixed
        if (numberOfArguments == 3 && strippedInputTypes.length >= 3) {
            IAType thirdArgument = strippedInputTypes[2];
            switch (thirdArgument.getTypeTag()) {
                case BOOLEAN:
                case ANY:
                    break;
                default:
                    return BuiltinType.ANULL;
            }
        }

        return isReturnNullable ? AUnionType.createNullableType(returnType) : returnType;
    }
}
