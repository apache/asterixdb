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
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import org.apache.hyracks.algebricks.core.algebra.properties.VariablePropagationPolicy;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalExpressionReferenceTransform;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class OrderOperator extends AbstractLogicalOperator {

    public interface IOrder {
        public enum OrderKind {
            FUNCTIONCALL,
            ASC,
            DESC
        };

        public Mutable<ILogicalExpression> getExpressionRef();

        public OrderKind getKind();
    }

    public static IOrder ASC_ORDER = new IOrder() {

        @Override
        public Mutable<ILogicalExpression> getExpressionRef() {
            return null;
        }

        @Override
        public OrderKind getKind() {
            return OrderKind.ASC;
        }

    };

    public static IOrder DESC_ORDER = new IOrder() {

        @Override
        public Mutable<ILogicalExpression> getExpressionRef() {
            return null;
        }

        @Override
        public OrderKind getKind() {
            return OrderKind.DESC;
        }
    };

    public class FunOrder implements IOrder {
        private final Mutable<ILogicalExpression> f;

        public FunOrder(Mutable<ILogicalExpression> f) {
            this.f = f;
        }

        @Override
        public Mutable<ILogicalExpression> getExpressionRef() {
            return f;
        }

        @Override
        public OrderKind getKind() {
            return OrderKind.FUNCTIONCALL;
        }

    };

    private final List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExpressions;
    // In case we can push down LIMIT information into this operator.
    protected final int topK;

    // These are pairs of type (comparison, expr) where comparison is
    // ASC or DESC or a boolean function of arity 2 that can take as
    // arguments results of expr.

    // TopK : -1 means there is no LIMIT push-down optimization applied to this operator.
    public OrderOperator() {
        orderExpressions = new ArrayList<Pair<IOrder, Mutable<ILogicalExpression>>>();
        topK = -1;
    }

    public OrderOperator(List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExpressions) {
        this(orderExpressions, -1);
    }

    public OrderOperator(List<Pair<IOrder, Mutable<ILogicalExpression>>> orderExpressions, int topK) {
        this.orderExpressions = orderExpressions;
        this.topK = topK;
    }

    @Override
    public LogicalOperatorTag getOperatorTag() {
        return LogicalOperatorTag.ORDER;
    }

    public List<Pair<IOrder, Mutable<ILogicalExpression>>> getOrderExpressions() {
        return orderExpressions;
    }

    @Override
    public void recomputeSchema() {
        schema = new ArrayList<LogicalVariable>(inputs.get(0).getValue().getSchema());
    }

    @Override
    public VariablePropagationPolicy getVariablePropagationPolicy() {
        return VariablePropagationPolicy.ALL;
    }

    @Override
    public boolean acceptExpressionTransform(ILogicalExpressionReferenceTransform visitor) throws AlgebricksException {
        boolean b = false;
        for (Pair<IOrder, Mutable<ILogicalExpression>> p : orderExpressions) {
            if (p.first.getKind() == OrderKind.FUNCTIONCALL) {
                FunOrder fo = (FunOrder) p.first;
                Mutable<ILogicalExpression> r1 = fo.getExpressionRef();
                if (visitor.transform(r1)) {
                    b = true;
                }
            }
            if (visitor.transform(p.second)) {
                b = true;
            }
        }
        return b;
    }

    @Override
    public <R, T> R accept(ILogicalOperatorVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visitOrderOperator(this, arg);
    }

    @Override
    public boolean isMap() {
        return false;
    }

    @Override
    public IVariableTypeEnvironment computeOutputTypeEnvironment(ITypingContext ctx) throws AlgebricksException {
        return createPropagatingAllInputsTypeEnvironment(ctx);
    }

    public int getTopK() {
        return topK;
    }
}
