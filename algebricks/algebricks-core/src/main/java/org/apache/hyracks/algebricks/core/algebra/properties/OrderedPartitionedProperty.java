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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class OrderedPartitionedProperty implements IPartitioningProperty {

    private ArrayList<OrderColumn> orderColumns;
    private INodeDomain domain;

    public OrderedPartitionedProperty(ArrayList<OrderColumn> orderColumns, INodeDomain domain) {
        this.domain = domain;
        this.orderColumns = orderColumns;
    }

    public ArrayList<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public ArrayList<LogicalVariable> getColumns() {
        ArrayList<LogicalVariable> cols = new ArrayList<LogicalVariable>(orderColumns.size());
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
        return getPartitioningType().toString() + orderColumns;
    }

    @Override
    public void normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses, List<FunctionalDependency> fds) {
        orderColumns = PropertiesUtil.replaceOrderColumnsByEqClasses(orderColumns, equivalenceClasses);
        orderColumns = PropertiesUtil.applyFDsToOrderColumns(orderColumns, fds);
    }

    @Override
    public void getColumns(Collection<LogicalVariable> columns) {
        for (OrderColumn oc : orderColumns) {
            columns.add(oc.getColumn());
        }
    }

    @Override
    public INodeDomain getNodeDomain() {
        return domain;
    }

    @Override
    public void setNodeDomain(INodeDomain domain) {
        this.domain = domain;
    }

}
