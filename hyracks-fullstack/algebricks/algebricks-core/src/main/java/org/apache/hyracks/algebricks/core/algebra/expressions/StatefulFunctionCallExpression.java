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
import org.apache.hyracks.algebricks.core.algebra.properties.IPropertiesComputer;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class StatefulFunctionCallExpression extends AbstractFunctionCallExpression {

    private final IPropertiesComputer propertiesComputer;

    public StatefulFunctionCallExpression(IFunctionInfo finfo, IPropertiesComputer propertiesComputer) {
        super(FunctionKind.STATEFUL, finfo);
        this.propertiesComputer = propertiesComputer;
    }

    public StatefulFunctionCallExpression(IFunctionInfo finfo, IPropertiesComputer propertiesComputer,
            List<Mutable<ILogicalExpression>> arguments) {
        super(FunctionKind.STATEFUL, finfo, arguments);
        this.propertiesComputer = propertiesComputer;
    }

    public StatefulFunctionCallExpression(IFunctionInfo finfo, IPropertiesComputer propertiesComputer,
            Mutable<ILogicalExpression>... expressions) {
        super(FunctionKind.STATEFUL, finfo, expressions);
        this.propertiesComputer = propertiesComputer;
    }

    @Override
    public StatefulFunctionCallExpression cloneExpression() {
        cloneAnnotations();
        List<Mutable<ILogicalExpression>> clonedArgs = cloneArguments();
        StatefulFunctionCallExpression clonedExpr =
                new StatefulFunctionCallExpression(finfo, propertiesComputer, clonedArgs);
        clonedExpr.setSourceLocation(sourceLoc);
        return clonedExpr;
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitStatefulFunctionCallExpression(this, arg);
    }

    public IPropertiesComputer getPropertiesComputer() {
        return propertiesComputer;
    }
}
