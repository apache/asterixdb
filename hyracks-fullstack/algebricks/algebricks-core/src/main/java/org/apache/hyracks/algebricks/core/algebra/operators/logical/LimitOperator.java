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
package org.apache.hyracks.algebricks.core.algebra.operators.logical;

import java.util.ArrayList;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class LimitOperator extends AbstractLogicalOperator {

    private final Mutable<ILogicalExpression> maxObjects; // optional, if not specified then offset is required
    private final Mutable<ILogicalExpression> offset; // optional if maxObjects is specified, required otherwise
    private final boolean topmost;

    public LimitOperator(ILogicalExpression maxObjectsExpr, ILogicalExpression offsetExpr, boolean topmost) {
        if (maxObjectsExpr == null && offsetExpr == null) {
            throw new IllegalArgumentException();
        }
        this.maxObjects = new MutableObject<>(maxObjectsExpr);
        this.offset = new MutableObject<>(offsetExpr);
        this.topmost = topmost;
    }

    public LimitOperator(ILogicalExpression maxObjects, ILogicalExpression offset) {
        this(maxObjects, offset, true);
    }

    public LimitOperator(ILogicalExpression maxObjectsExpr, boolean topmost) {
        this(maxObjectsExpr, null, topmost);
    }

    public LimitOperator(ILogicalExpression maxObjects) {
        this(maxObjects, true);
    }

    public boolean hasMaxObjects() {
        return maxObjects.getValue() != null;
    }

    public Mutable<ILogicalExpression> getMaxObjects() {
        return maxObjects;
    }

    public boolean hasOffset() {
        return offset.getValue() != null;
    }

    public Mutable<ILogicalExpression> getOffset() {
        return offset;
    }

    public boolean isTopmostLimitOp() {
        return topmost;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<>();
        schema.addAll(inputs.get(0).getValue().getSchema());
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitLimitOperator(this, arg);
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        if (hasMaxObjects()) {
            b = visitor.transform(maxObjects);
        }
        if (hasOffset()) {
            b |= visitor.transform(offset);
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
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }
}
