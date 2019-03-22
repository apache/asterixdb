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
package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class UnnestingFunctionCallExpression extends AbstractFunctionCallExpression {

    private boolean returnsUniqueValues;

    public UnnestingFunctionCallExpression(IFunctionInfo finfo) {
        super(FunctionKind.UNNEST, finfo);
    }

    public UnnestingFunctionCallExpression(IFunctionInfo finfo, List<Mutable<ILogicalExpression>> arguments) {
        super(FunctionKind.UNNEST, finfo, arguments);
    }

    public UnnestingFunctionCallExpression(IFunctionInfo finfo, Mutable<ILogicalExpression>... expressions) {
        super(FunctionKind.UNNEST, finfo, expressions);
    }

    @Override
    public UnnestingFunctionCallExpression cloneExpression() {
        cloneAnnotations();
        List<Mutable<ILogicalExpression>> clonedArgs = cloneArguments();
        UnnestingFunctionCallExpression ufce = new UnnestingFunctionCallExpression(finfo, clonedArgs);
        ufce.setReturnsUniqueValues(returnsUniqueValues);
        ufce.setOpaqueParameters(this.getOpaqueParameters());
        ufce.setSourceLocation(sourceLoc);
        return ufce;
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitUnnestingFunctionCallExpression(this, arg);
    }

    public void setReturnsUniqueValues(boolean returnsUniqueValues) {
        this.returnsUniqueValues = returnsUniqueValues;
    }

    public boolean returnsUniqueValues() {
        return returnsUniqueValues;
    }

}
