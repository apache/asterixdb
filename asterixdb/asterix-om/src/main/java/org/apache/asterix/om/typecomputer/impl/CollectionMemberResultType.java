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

import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class CollectionMemberResultType extends AbstractResultTypeComputer {
    public static final CollectionMemberResultType INSTANCE = new CollectionMemberResultType(false, false);

    public static final CollectionMemberResultType INSTANCE_NULLABLE = new CollectionMemberResultType(true, false);

    public static final CollectionMemberResultType INSTANCE_MISSABLE = new CollectionMemberResultType(false, true);

    private final boolean nullable;

    private final boolean missable;

    private CollectionMemberResultType(boolean nullable, boolean missable) {
        this.missable = missable;
        this.nullable = nullable;
    }

    @Override
    protected void checkArgType(FunctionIdentifier funcId, int argIndex, IAType type, SourceLocation sourceLoc)
            throws AlgebricksException {
        if (argIndex == 0) {
            ATypeTag actualTypeTag = type.getTypeTag();
            if (!type.getTypeTag().isListType()) {
                throw new TypeMismatchException(sourceLoc, actualTypeTag, ATypeTag.MULTISET, ATypeTag.ARRAY);
            }
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType type = strippedInputTypes[0];
        if (!type.getTypeTag().isListType()) {
            return BuiltinType.ANY;
        }
        IAType itemType = ((AbstractCollectionType) type).getItemType();
        if (nullable) {
            itemType = AUnionType.createNullableType(itemType);
        }
        if (missable) {
            itemType = AUnionType.createMissableType(itemType);
        }
        return itemType;
    }
}
