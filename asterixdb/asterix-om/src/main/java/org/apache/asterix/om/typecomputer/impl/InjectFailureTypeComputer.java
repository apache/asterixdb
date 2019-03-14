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
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * The first argument of INJECT_FAILURE can be any data model instance and will be passed verbatim to the
 * caller. The second argument is a boolean that determines if the invocation throws an exception.
 *
 * Consequently {@link AbstractResultTypeComputer#checkArgType(FunctionIdentifier, int, IAType, SourceLocation)} validates that the second argument is a
 * boolean and {@link #getResultType(ILogicalExpression, IAType...)} returns the type of the first
 * argument.
 */
public class InjectFailureTypeComputer extends AbstractResultTypeComputer {

    public static final InjectFailureTypeComputer INSTANCE = new InjectFailureTypeComputer();

    @Override
    protected void checkArgType(FunctionIdentifier funcId, int argIndex, IAType type, SourceLocation sourceLoc)
            throws AlgebricksException {
        if (argIndex == 1 && type.getTypeTag() != ATypeTag.BOOLEAN) {
            throw new TypeMismatchException(sourceLoc, funcId, argIndex, type.getTypeTag(), ATypeTag.BOOLEAN);
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        return strippedInputTypes[0];
    }
}
