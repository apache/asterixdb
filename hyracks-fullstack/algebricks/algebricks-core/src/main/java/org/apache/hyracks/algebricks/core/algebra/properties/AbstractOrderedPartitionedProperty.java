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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

abstract class AbstractOrderedPartitionedProperty implements IPartitioningProperty {

    protected final List<OrderColumn> orderColumns;

    protected final RangeMap rangeMap;

    protected INodeDomain domain;

    AbstractOrderedPartitionedProperty(List<OrderColumn> orderColumns, INodeDomain domain) {
        this(orderColumns, domain, null);
    }

    AbstractOrderedPartitionedProperty(List<OrderColumn> orderColumns, INodeDomain domain, RangeMap rangeMap) {
        this.orderColumns = orderColumns;
        this.domain = domain;
        this.rangeMap = rangeMap;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public List<LogicalVariable> getColumns() {
        ArrayList<LogicalVariable> cols = new ArrayList<>(orderColumns.size());
        for (OrderColumn oc : orderColumns) {
            cols.add(oc.getColumn());
        }
        return cols;
    }

    @Override
    public String toString() {
        return getPartitioningType().toString() + orderColumns + " domain:" + domain
                + (rangeMap != null ? " range-map:" + rangeMap : "");
    }

    @Override
    public void getColumns(Collection<LogicalVariable> columns) {
        for (OrderColumn oc : orderColumns) {
            columns.add(oc.getColumn());
        }
    }

    public RangeMap getRangeMap() {
        return rangeMap;
    }

    @Override
    public INodeDomain getNodeDomain() {
        return domain;
    }

    @Override
    public void setNodeDomain(INodeDomain domain) {
        this.domain = domain;
    }

    @Override
    public IPartitioningProperty substituteColumnVars(Map<LogicalVariable, LogicalVariable> varMap) {
        boolean applied = false;
        List<OrderColumn> newOrderColumns = new ArrayList<>(orderColumns.size());
        for (OrderColumn orderColumn : orderColumns) {
            LogicalVariable columnVar = orderColumn.getColumn();
            LogicalVariable newColumnVar = varMap.get(columnVar);
            if (newColumnVar != null) {
                applied = true;
            } else {
                newColumnVar = columnVar;
            }
            newOrderColumns.add(new OrderColumn(newColumnVar, orderColumn.getOrder()));
        }
        return applied ? newInstance(newOrderColumns, domain, rangeMap) : this;
    }

    @Override
    public IPartitioningProperty clonePartitioningProperty() {
        return newInstance(new ArrayList<>(orderColumns), domain, rangeMap);
    }

    protected abstract AbstractOrderedPartitionedProperty newInstance(List<OrderColumn> columns, INodeDomain domain,
            RangeMap rangeMap);
}
