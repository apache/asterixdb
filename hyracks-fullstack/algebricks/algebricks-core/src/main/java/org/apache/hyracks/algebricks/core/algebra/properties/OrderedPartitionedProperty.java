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
import org.apache.hyracks.api.dataflow.value.IRangeMap;
import org.apache.hyracks.api.dataflow.value.IRangePartitionType.RangePartitioningType;
import org.apache.hyracks.dataflow.std.base.RangeId;

public class OrderedPartitionedProperty implements IPartitioningProperty {

    private List<OrderColumn> orderColumns;
    private INodeDomain domain;
    private RangeId rangeId;
    private RangePartitioningType rangeType;
    private IRangeMap rangeMapHint;

    public OrderedPartitionedProperty(List<OrderColumn> orderColumns, INodeDomain domain, RangeId rangeId,
            RangePartitioningType rangeType, IRangeMap rangeMapHint) {
        this.domain = domain;
        this.orderColumns = orderColumns;
        this.rangeId = rangeId;
        this.rangeType = rangeType;
        this.rangeMapHint = rangeMapHint;
    }

    public OrderedPartitionedProperty(List<OrderColumn> orderColumns, INodeDomain domain, RangeId rangeId) {
        this(orderColumns, domain, rangeId, RangePartitioningType.PROJECT, null);
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

    public RangePartitioningType getRangePartitioningType() {
        return rangeType;
    }

    @Override
    public String toString() {
        return getPartitioningType().toString() + " Column(s): " + orderColumns + " Range Type: " + rangeType;
    }

    @Override
    public IPartitioningProperty normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        List<OrderColumn> columns = PropertiesUtil.replaceOrderColumnsByEqClasses(orderColumns, equivalenceClasses);
        columns = PropertiesUtil.applyFDsToOrderColumns(columns, fds);
        return new OrderedPartitionedProperty(columns, domain, rangeId);
    }

    @Override
    public void getColumns(Collection<LogicalVariable> columns) {
        for (OrderColumn oc : orderColumns) {
            columns.add(oc.getColumn());
        }
    }

    public RangeId getRangeId() {
        return rangeId;
    }

    public IRangeMap getRangeMapHint() {
        return rangeMapHint;
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
    public void substituteColumnVars(Map<LogicalVariable, LogicalVariable> varMap) {
        for (OrderColumn orderColumn : orderColumns) {
            if (varMap.containsKey(orderColumn.getColumn())) {
                orderColumn.setColumn(varMap.get(orderColumn.getColumn()));
            }
        }
    }

}
