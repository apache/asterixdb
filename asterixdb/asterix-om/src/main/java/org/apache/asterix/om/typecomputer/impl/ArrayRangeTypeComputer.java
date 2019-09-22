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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class ArrayRangeTypeComputer extends AbstractResultTypeComputer {

    public static final ArrayRangeTypeComputer INSTANCE = new ArrayRangeTypeComputer();
    public static final AOrderedListType LONG_LIST = new AOrderedListType(BuiltinType.AINT64, null);
    public static final AOrderedListType DOUBLE_LIST = new AOrderedListType(BuiltinType.ADOUBLE, null);

    private ArrayRangeTypeComputer() {
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType startNum = strippedInputTypes[0];
        IAType endNum = strippedInputTypes[1];
        IAType step = strippedInputTypes.length == 3 ? strippedInputTypes[2] : null;
        if (ATypeHierarchy.canPromote(startNum.getTypeTag(), ATypeTag.BIGINT)
                && ATypeHierarchy.canPromote(endNum.getTypeTag(), ATypeTag.BIGINT)
                && (step == null || ATypeHierarchy.canPromote(step.getTypeTag(), ATypeTag.BIGINT))) {
            return LONG_LIST;
        } else if (ATypeHierarchy.canPromote(startNum.getTypeTag(), ATypeTag.DOUBLE)
                && ATypeHierarchy.canPromote(endNum.getTypeTag(), ATypeTag.DOUBLE)
                && (step == null || ATypeHierarchy.canPromote(step.getTypeTag(), ATypeTag.DOUBLE))) {
            // even if the args types are valid, range function might return NULL if the doubles turn out to be Nan/Inf
            return AUnionType.createNullableType(DOUBLE_LIST);
        } else {
            return BuiltinType.ANY;
        }
    }
}
