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
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class IfNanOrInfTypeComputer extends AbstractResultTypeComputer {

    public static final IfNanOrInfTypeComputer INSTANCE = new IfNanOrInfTypeComputer();

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        if (strippedInputTypes.length == 0) {
            return BuiltinType.ANULL;
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
            case ANY:
            case MISSING:
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
