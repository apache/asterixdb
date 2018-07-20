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

import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

/**
 * A type computer that returns the same list type as the presumably input list at argument 0. If the argument is not a
 * list, it returns "ANY".
 */
public class AListFirstTypeComputer extends AbstractResultTypeComputer {
    public static final AListFirstTypeComputer INSTANCE = new AListFirstTypeComputer(false, false);
    public static final AListFirstTypeComputer INSTANCE_FLATTEN = new AListFirstTypeComputer(true, true);

    private final boolean makeOpen;
    private final boolean makeNullable;

    private AListFirstTypeComputer(boolean makeOpen, boolean makeNullable) {
        this.makeOpen = makeOpen;
        this.makeNullable = makeNullable;
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType argType = strippedInputTypes[0];
        switch (argType.getTypeTag()) {
            case ARRAY:
            case MULTISET:
                if (makeOpen) {
                    argType = DefaultOpenFieldType.getDefaultOpenFieldType(argType.getTypeTag());
                }
                if (makeNullable) {
                    return AUnionType.createNullableType(argType);
                } else {
                    return argType;
                }
            default:
                return BuiltinType.ANY;
        }
    }
}
