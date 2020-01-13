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

import static org.apache.asterix.om.types.BuiltinType.ADOUBLE;
import static org.apache.asterix.om.types.BuiltinType.AINT8;

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class NumericUnaryTypeComputer extends AbstractResultTypeComputer {

    public static final NumericUnaryTypeComputer INSTANCE = new NumericUnaryTypeComputer(null);
    public static final NumericUnaryTypeComputer INSTANCE_INT8 = new NumericUnaryTypeComputer(AINT8);
    public static final NumericUnaryTypeComputer INSTANCE_DOUBLE = new NumericUnaryTypeComputer(ADOUBLE);

    // when returnType is null, the function returns the same type as the input argument
    private final IAType returnType;

    private NumericUnaryTypeComputer(IAType returnType) {
        this.returnType = returnType;
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        ATypeTag tag = strippedInputTypes[0].getTypeTag();
        switch (tag) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return returnType == null ? strippedInputTypes[0] : returnType;
            case ANY:
                return returnType == null ? BuiltinType.ANY : AUnionType.createUnknownableType(returnType);
            default:
                // null for all other invalid types
                return BuiltinType.ANULL;
        }
    }
}
