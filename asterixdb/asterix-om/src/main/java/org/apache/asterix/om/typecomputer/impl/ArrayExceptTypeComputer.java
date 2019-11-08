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
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class ArrayExceptTypeComputer extends AbstractResultTypeComputer {

    public static final ArrayExceptTypeComputer INSTANCE = new ArrayExceptTypeComputer();

    private ArrayExceptTypeComputer() {
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType arg1Type = strippedInputTypes[0];
        IAType arg2Type = strippedInputTypes[1];

        switch (arg1Type.getTypeTag()) {
            case ARRAY:
            case MULTISET:
            case ANY:
                switch (arg2Type.getTypeTag()) {
                    case ARRAY:
                    case MULTISET:
                    case ANY:
                        return arg1Type;
                    default:
                        return BuiltinType.ANULL;
                }
            default:
                return BuiltinType.ANULL;
        }
    }
}
