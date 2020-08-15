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

package org.apache.hyracks.algebricks.core.algebra.properties;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;

public final class IntervalColumn {

    private LogicalVariable startColumn;

    private LogicalVariable endColumn;

    private OrderKind order;

    public IntervalColumn(LogicalVariable startColumn, LogicalVariable endColumn, OrderKind order) {
        this.startColumn = startColumn;
        this.endColumn = endColumn;
        this.order = order;
    }

    public LogicalVariable getStartColumn() {
        return startColumn;
    }

    public LogicalVariable getEndColumn() {
        return endColumn;
    }

    public OrderKind getOrder() {
        return order;
    }

    public void setStartColumn(LogicalVariable column) {
        this.startColumn = column;
    }

    public void setEndColumn(LogicalVariable column) {
        this.endColumn = column;
    }

    public void setOrder(OrderKind order) {
        this.order = order;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IntervalColumn)) {
            return false;
        } else {
            IntervalColumn ic = (IntervalColumn) obj;
            return startColumn.equals(ic.getStartColumn()) && endColumn.equals(ic.getEndColumn())
                    && order == ic.getOrder();
        }
    }

    @Override
    public String toString() {
        return "{" + startColumn + "," + endColumn + "," + order + "}";
    }
}
