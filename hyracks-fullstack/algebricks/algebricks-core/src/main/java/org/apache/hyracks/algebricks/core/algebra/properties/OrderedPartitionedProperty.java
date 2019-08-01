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

import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class OrderedPartitionedProperty implements IPartitioningProperty {

    private final List<OrderColumn> orderColumns;
    private final RangeMap rangeMap;
    private INodeDomain domain;

    public OrderedPartitionedProperty(List<OrderColumn> orderColumns, INodeDomain domain) {
        this(orderColumns, domain, null);
    }

    public OrderedPartitionedProperty(List<OrderColumn> orderColumns, INodeDomain domain, RangeMap rangeMap) {
        this.domain = domain;
        this.orderColumns = orderColumns;
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
    public PartitioningType getPartitioningType() {
        return PartitioningType.ORDERED_PARTITIONED;
    }

    @Override
    public String toString() {
        return getPartitioningType().toString() + orderColumns + " domain:" + domain
                + (rangeMap != null ? " range-map:" + rangeMap : "");
    }

    @Override
    public IPartitioningProperty normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        List<OrderColumn> columns = PropertiesUtil.replaceOrderColumnsByEqClasses(orderColumns, equivalenceClasses);
        columns = PropertiesUtil.applyFDsToOrderColumns(columns, fds);
        return new OrderedPartitionedProperty(columns, domain, rangeMap);
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
        return applied ? new OrderedPartitionedProperty(newOrderColumns, domain, rangeMap) : this;
    }

    @Override
    public IPartitioningProperty clonePartitioningProperty() {
        return new OrderedPartitionedProperty(new ArrayList<>(orderColumns), domain, rangeMap);
    }
}
