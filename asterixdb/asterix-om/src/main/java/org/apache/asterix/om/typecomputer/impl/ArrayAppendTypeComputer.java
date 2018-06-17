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
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class ArrayAppendTypeComputer extends AbstractResultTypeComputer {

    public static final ArrayAppendTypeComputer INSTANCE = new ArrayAppendTypeComputer();

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        if (strippedInputTypes.length < 2) {
            String functionName = ((AbstractFunctionCallExpression) expr).getFunctionIdentifier().getName();
            throw new CompilationException(ErrorCode.COMPILATION_INVALID_NUM_OF_ARGS, expr.getSourceLocation(), 2,
                    functionName);
        }
        // type tag at [0] should be array or multiset.
        ATypeTag typeTag = strippedInputTypes[0].getTypeTag();
        if (typeTag == ATypeTag.ARRAY) {
            return DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
        } else if (typeTag == ATypeTag.MULTISET) {
            return DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
        } else {
            return BuiltinType.ANY;
        }
    }
}
