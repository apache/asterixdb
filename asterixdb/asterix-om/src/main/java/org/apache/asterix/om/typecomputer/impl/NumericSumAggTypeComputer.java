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

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class NumericSumAggTypeComputer extends AggregateResultTypeComputer {
    public static final NumericSumAggTypeComputer INSTANCE = new NumericSumAggTypeComputer();

    private NumericSumAggTypeComputer() {
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        ATypeTag tag = strippedInputTypes[0].getTypeTag();
        switch (tag) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return AUnionType.createNullableType(BuiltinType.AINT64);
            case FLOAT:
            case DOUBLE:
                return AUnionType.createNullableType(BuiltinType.ADOUBLE);
            case ANY:
                return BuiltinType.ANY;
            default:
                // All other possible cases.
                return BuiltinType.ANULL;
        }

    }
}
