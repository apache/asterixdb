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
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ScalarVersionOfAggregateResultType extends AbstractResultTypeComputer {

    private final AggregateResultTypeComputer aggResultTypeComputer;

    public ScalarVersionOfAggregateResultType(AggregateResultTypeComputer aggResultTypeComputer) {
        this.aggResultTypeComputer = aggResultTypeComputer;
    }

    @Override
    protected void checkArgType(FunctionIdentifier funcId, int argIndex, IAType argType, SourceLocation sourceLoc)
            throws AlgebricksException {
        if (argIndex == 0) {
            switch (argType.getTypeTag()) {
                case ARRAY:
                case MULTISET:
                    AbstractCollectionType act = (AbstractCollectionType) argType;
                    aggResultTypeComputer.checkArgType(funcId, argIndex, act.getItemType(), sourceLoc);
                    break;
            }
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType argType = strippedInputTypes[0];
        switch (argType.getTypeTag()) {
            case ARRAY:
            case MULTISET:
                AbstractCollectionType act = (AbstractCollectionType) argType;
                IAType[] strippedInputTypes2 = strippedInputTypes.clone();
                strippedInputTypes2[0] = act.getItemType();
                IAType resultType = aggResultTypeComputer.getResultType(expr, strippedInputTypes2);
                switch (resultType.getTypeTag()) {
                    case NULL:
                    case MISSING:
                    case ANY:
                        return resultType;
                    case UNION:
                        return AUnionType.createUnknownableType(((AUnionType) resultType).getActualType());
                    default:
                        return AUnionType.createUnknownableType(resultType);
                }
            default:
                return BuiltinType.ANY;
        }
    }
}
