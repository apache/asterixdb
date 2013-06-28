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
package edu.uci.ics.hyracks.algebricks.core.algebra.properties;

import java.util.Collection;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;

public final class LocalOrderProperty implements ILocalStructuralProperty {

    private OrderColumn orderColumn;

    public LocalOrderProperty(OrderColumn orderColumn) {
        this.orderColumn = orderColumn;
    }

    public OrderColumn getOrderColumn() {
        return orderColumn;
    }

    public void setOrderColumn(OrderColumn orderColumn) {
        this.orderColumn = orderColumn;
    }

    public LogicalVariable getColumn() {
        return orderColumn.getColumn();
    }

    public OrderKind getOrder() {
        return orderColumn.getOrder();
    }

    @Override
    public PropertyType getPropertyType() {
        return PropertyType.LOCAL_ORDER_PROPERTY;
    }

    @Override
    public void getColumns(Collection<LogicalVariable> columns) {
        columns.add(getColumn());
    }

    @Override
    public String toString() {
        return orderColumn.toString();
    }

    @Override
    public void getVariables(Collection<LogicalVariable> variables) {
        variables.add(orderColumn.getColumn());
    }

}
