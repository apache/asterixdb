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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionReference;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class LimitOperator extends AbstractLogicalOperator {

    private final LogicalExpressionReference maxObjects; // mandatory
    private final LogicalExpressionReference offset; // optional
    private boolean topmost;

    public LimitOperator(ILogicalExpression maxObjectsExpr, ILogicalExpression offsetExpr, boolean topmost) {
        this.maxObjects = new LogicalExpressionReference(maxObjectsExpr);
        this.offset = new LogicalExpressionReference(offsetExpr);
        this.topmost = topmost;
    }

    public LimitOperator(ILogicalExpression maxObjectsExpr, boolean topmost) {
        this(maxObjectsExpr, null, topmost);
    }

    public LimitOperator(ILogicalExpression maxObjects, ILogicalExpression offset) {
        this(maxObjects, offset, true);
    }

    public LimitOperator(ILogicalExpression maxObjects) {
        this(maxObjects, null, true);
    }

    public LogicalExpressionReference getMaxObjects() {
        return maxObjects;
    }

    public LogicalExpressionReference getOffset() {
        return offset;
    }

    public boolean isTopmostLimitOp() {
        return topmost;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>();
        schema.addAll(inputs.get(0).getOperator().getSchema());
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitLimitOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        if (visitor.transform(maxObjects)) {
            b = true;
        }
        if (offset.getExpression() != null) {
            if (visitor.transform(offset)) {
                b = true;
            }
        }
        return b;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.LIMIT;
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean isMap() {
        return true;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

}
