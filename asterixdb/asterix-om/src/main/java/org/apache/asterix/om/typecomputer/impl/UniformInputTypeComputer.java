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
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

/**
 * This type computer functions based on 2 arguments, the {@code inputType} and the {@code outputType}. It checks that
 * all the input types conform to the provided {@code inputType}, and if so, it returns the {@code outputType},
 * otherwise null type is returned.
 */
public class UniformInputTypeComputer extends AbstractResultTypeComputer {

    public static final UniformInputTypeComputer STRING_STRING_INSTANCE =
            new UniformInputTypeComputer(BuiltinType.ASTRING, BuiltinType.ASTRING);
    public static final UniformInputTypeComputer STRING_BOOLEAN_INSTANCE =
            new UniformInputTypeComputer(BuiltinType.ASTRING, BuiltinType.ABOOLEAN);
    public static final UniformInputTypeComputer STRING_INT32_INSTANCE =
            new UniformInputTypeComputer(BuiltinType.ASTRING, BuiltinType.AINT32);
    public static final UniformInputTypeComputer STRING_INT64_INSTANCE =
            new UniformInputTypeComputer(BuiltinType.ASTRING, BuiltinType.AINT64);
    public static final UniformInputTypeComputer STRING_STRING_LIST_INSTANCE = new UniformInputTypeComputer(
            BuiltinType.ASTRING, new AOrderedListType(BuiltinType.ASTRING, BuiltinType.ASTRING.getTypeName()));
    public static final UniformInputTypeComputer STRING_INT64_LIST_INSTANCE = new UniformInputTypeComputer(
            BuiltinType.ASTRING, new AOrderedListType(BuiltinType.AINT64, BuiltinType.AINT64.getTypeName()));

    private final IAType inputType;
    private final IAType outputType;

    private UniformInputTypeComputer(IAType inputType, IAType outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    @Override
    public IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        // all args are expected to be of type inputType
        for (IAType actualType : strippedInputTypes) {
            if (actualType.getTypeTag() != inputType.getTypeTag() && actualType.getTypeTag() != ATypeTag.ANY) {
                return BuiltinType.ANULL;
            }
        }

        return outputType;
    }
}
