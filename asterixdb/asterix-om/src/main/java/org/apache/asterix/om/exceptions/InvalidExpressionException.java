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

package org.apache.asterix.om.exceptions;

import static org.apache.asterix.om.exceptions.ExceptionUtil.indexToPosition;
import static org.apache.asterix.om.exceptions.ExceptionUtil.toExpectedTypeString;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class InvalidExpressionException extends CompilationException {
    private static final long serialVersionUID = -3601791480135148643L;

    public InvalidExpressionException(FunctionIdentifier fid, int index, ILogicalExpression actualExpr,
            LogicalExpressionTag... exprKinds) {
        super(ErrorCode.COMPILATION_INVALID_EXPRESSION, fid.getName(), indexToPosition(index), actualExpr.toString(),
                toExpectedTypeString(exprKinds));
    }

    public InvalidExpressionException(SourceLocation sourceLoc, FunctionIdentifier fid, int index,
            ILogicalExpression actualExpr, LogicalExpressionTag... exprKinds) {
        super(ErrorCode.COMPILATION_INVALID_EXPRESSION, sourceLoc, fid.getName(), indexToPosition(index),
                actualExpr.toString(), toExpectedTypeString(exprKinds));
    }

    @Deprecated
    public InvalidExpressionException(String functionName, int index, ILogicalExpression actualExpr,
            LogicalExpressionTag... exprKinds) {
        super(ErrorCode.COMPILATION_INVALID_EXPRESSION, functionName, indexToPosition(index), actualExpr.toString(),
                toExpectedTypeString(exprKinds));
    }

    @Deprecated
    public InvalidExpressionException(SourceLocation sourceLoc, String functionName, int index,
            ILogicalExpression actualExpr, LogicalExpressionTag... exprKinds) {
        super(ErrorCode.COMPILATION_INVALID_EXPRESSION, sourceLoc, functionName, indexToPosition(index),
                actualExpr.toString(), toExpectedTypeString(exprKinds));
    }
}
