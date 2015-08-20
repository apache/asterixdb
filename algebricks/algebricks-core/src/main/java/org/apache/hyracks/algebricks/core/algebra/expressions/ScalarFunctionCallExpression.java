/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.core.algebra.expressions;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;

public class ScalarFunctionCallExpression extends AbstractFunctionCallExpression {

    public ScalarFunctionCallExpression(IFunctionInfo finfo) {
        super(FunctionKind.SCALAR, finfo);
    }

    public ScalarFunctionCallExpression(IFunctionInfo finfo, List<Mutable<ILogicalExpression>> arguments) {
        super(FunctionKind.SCALAR, finfo, arguments);
    }

    public ScalarFunctionCallExpression(IFunctionInfo finfo, Mutable<ILogicalExpression>... expressions) {
        super(FunctionKind.SCALAR, finfo, expressions);
    }

    @Override
    public ScalarFunctionCallExpression cloneExpression() {
        List<Mutable<ILogicalExpression>> clonedArgs = cloneArguments();
        ScalarFunctionCallExpression funcExpr = new ScalarFunctionCallExpression(finfo, clonedArgs);
        funcExpr.getAnnotations().putAll(cloneAnnotations());
        funcExpr.setOpaqueParameters(this.getOpaqueParameters());
        return funcExpr;
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitScalarFunctionCallExpression(this, arg);
    }

}
