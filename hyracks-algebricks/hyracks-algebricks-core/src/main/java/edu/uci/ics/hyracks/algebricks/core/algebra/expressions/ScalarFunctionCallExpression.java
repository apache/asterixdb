/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class ScalarFunctionCallExpression extends AbstractFunctionCallExpression {

    public ScalarFunctionCallExpression(IFunctionInfo finfo) {
        super(FunctionKind.SCALAR, finfo);
    }

    public ScalarFunctionCallExpression(IFunctionInfo finfo, List<LogicalExpressionReference> arguments) {
        super(FunctionKind.SCALAR, finfo, arguments);
    }

    public ScalarFunctionCallExpression(IFunctionInfo finfo, LogicalExpressionReference... expressions) {
        super(FunctionKind.SCALAR, finfo, expressions);
    }

    @Override
    public ScalarFunctionCallExpression cloneExpression() {
        cloneAnnotations();
        List<LogicalExpressionReference> clonedArgs = cloneArguments();
        return new ScalarFunctionCallExpression(finfo, clonedArgs);
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitScalarFunctionCallExpression(this, arg);
    }

}
