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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class CollectionMemberResultType extends AbstractResultTypeComputer {

    public static final CollectionMemberResultType INSTANCE = new CollectionMemberResultType();

    protected CollectionMemberResultType() {

    }

    @Override
    protected void checkArgType(int argIndex, IAType type) throws AlgebricksException {
        if (type.getTypeTag() != ATypeTag.UNORDEREDLIST && type.getTypeTag() != ATypeTag.ORDEREDLIST) {
            throw new AlgebricksException(
                    "Unnest or index access expects the input to be a collection, but it was of type "
                    + type);
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType type = strippedInputTypes[0];
        if (type.getTypeTag() == ATypeTag.ANY) {
            return BuiltinType.ANY;
        }
        return ((AbstractCollectionType) type).getItemType();
    }

}
