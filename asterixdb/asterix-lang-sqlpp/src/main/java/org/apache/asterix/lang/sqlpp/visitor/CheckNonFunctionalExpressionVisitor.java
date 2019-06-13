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
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.expression.CallExpr;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.lang.sqlpp.visitor.base.AbstractSqlppContainsExpressionVisitor;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

/**
 * Checks whether given expression is non-functional (i.e. whether it calls a non-functional function)
 */
public final class CheckNonFunctionalExpressionVisitor extends AbstractSqlppContainsExpressionVisitor<Void> {
    @Override
    public Boolean visit(CallExpr callExpr, Void arg) throws CompilationException {
        FunctionSignature fs = callExpr.getFunctionSignature();
        IFunctionInfo fi = FunctionUtil.getBuiltinFunctionInfo(fs.getName(), fs.getArity());
        // TODO: all external functions are considered functional for now.
        // we'll need to revisit this code once we enable non-functional in ExternalFunctionInfo
        if (fi != null && !fi.isFunctional()) {
            return true;
        }
        return super.visit(callExpr, arg);
    }
}
