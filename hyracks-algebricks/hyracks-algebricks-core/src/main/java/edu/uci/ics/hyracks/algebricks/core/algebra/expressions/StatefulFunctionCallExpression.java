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
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPropertiesComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionVisitor;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class StatefulFunctionCallExpression extends AbstractFunctionCallExpression {

    private final IPropertiesComputer propertiesComputer;

    public StatefulFunctionCallExpression(IFunctionInfo finfo, IPropertiesComputer propertiesComputer) {
        super(FunctionKind.STATEFUL, finfo);
        this.propertiesComputer = propertiesComputer;
    }

    public StatefulFunctionCallExpression(IFunctionInfo finfo, IPropertiesComputer propertiesComputer,
            List<LogicalExpressionReference> arguments) {
        super(FunctionKind.STATEFUL, finfo, arguments);
        this.propertiesComputer = propertiesComputer;
    }

    public StatefulFunctionCallExpression(IFunctionInfo finfo, IPropertiesComputer propertiesComputer,
            LogicalExpressionReference... expressions) {
        super(FunctionKind.STATEFUL, finfo, expressions);
        this.propertiesComputer = propertiesComputer;
    }

    @Override
    public StatefulFunctionCallExpression cloneExpression() {
        cloneAnnotations();
        List<LogicalExpressionReference> clonedArgs = cloneArguments();
        return new StatefulFunctionCallExpression(finfo, propertiesComputer, clonedArgs);
    }

    // can be null
    public IPartitioningProperty getRequiredPartitioningProperty() {
        return propertiesComputer.computePartitioningProperty(this);
    }

    @Override
    public <R, T> R accept(ILogicalExpressionVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitStatefulFunctionCallExpression(this, arg);
    }
}
