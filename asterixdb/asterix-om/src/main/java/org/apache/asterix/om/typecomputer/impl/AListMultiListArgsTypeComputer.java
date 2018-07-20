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

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

/**
 * Returns a list that is missable/nullable. All input lists should have the same type. This is checked during runtime.
 * List type is taken from one of the input args, [0]
 */
public class AListMultiListArgsTypeComputer extends AbstractResultTypeComputer {
    public static final AListMultiListArgsTypeComputer INSTANCE = new AListMultiListArgsTypeComputer();

    private AListMultiListArgsTypeComputer() {
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        if (strippedInputTypes.length < 2) {
            String functionName = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier().getName();
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, expr.getSourceLocation(),
                    functionName);
        }

        IAType listType = strippedInputTypes[0];
        if (listType.getTypeTag().isListType()) {
            listType = DefaultOpenFieldType.getDefaultOpenFieldType(listType.getTypeTag());
            return AUnionType.createUnknownableType(listType);
        } else {
            return BuiltinType.ANY;
        }
    }
}
