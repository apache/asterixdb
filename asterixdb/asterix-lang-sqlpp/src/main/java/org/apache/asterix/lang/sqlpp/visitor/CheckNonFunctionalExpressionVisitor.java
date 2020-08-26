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

package org.apache.asterix.lang.sqlpp.visitor;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppContainsExpressionVisitor;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

/**
 * Checks whether given expression is non-functional (i.e. whether it calls a non-functional function)
 */
public final class CheckNonFunctionalExpressionVisitor extends AbstractSqlppContainsExpressionVisitor<Void> {

    private final MetadataProvider metadataProvider;

    public CheckNonFunctionalExpressionVisitor(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    @Override
    public Boolean visit(CallExpr callExpr, Void arg) throws CompilationException {
        FunctionSignature fs = callExpr.getFunctionSignature();
        IFunctionInfo finfo = BuiltinFunctions.getBuiltinFunctionInfo(fs.createFunctionIdentifier());
        if (finfo != null) {
            if (!finfo.isFunctional()) {
                return true;
            }
        } else {
            Function function;
            try {
                function = metadataProvider.lookupUserDefinedFunction(fs);
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.METADATA_ERROR, e, callExpr.getSourceLocation(), e.toString());
            }
            if (function == null || function.getDeterministic() == null) {
                // fail if function not found because all functions must have been resolved at this point
                // fail if function does not define deterministic property (because it's a SQL++ function
                // and they were supposed to be inlined at this point)
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, callExpr.getSourceLocation(), fs);
            }
            if (!function.getDeterministic()) {
                return true;
            }
        }
        return super.visit(callExpr, arg);
    }
}
