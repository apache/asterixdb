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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;

public final class LocalOrderProperty implements ILocalStructuralProperty {

    private List<OrderColumn> orderColumns;

    public LocalOrderProperty(List<OrderColumn> orderColumn) {
        this.orderColumns = orderColumn;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public void setOrderColumns(List<OrderColumn> orderColumn) {
        this.orderColumns = orderColumn;
    }

    public List<LogicalVariable> getColumns() {
        List<LogicalVariable> orderVars = new ArrayList<>();
        for (OrderColumn oc : orderColumns) {
            orderVars.add(oc.getColumn());
        }
        return orderVars;
    }

    public List<OrderKind> getOrders() {
        List<OrderKind> orderKinds = new ArrayList<>();
        for (OrderColumn oc : orderColumns) {
            orderKinds.add(oc.getOrder());
        }
        return orderKinds;
    }

    @Override
    public PropertyType getPropertyType() {
        return PropertyType.LOCAL_ORDER_PROPERTY;
    }

    @Override
    public void getColumns(Collection<LogicalVariable> columns) {
        columns.addAll(getColumns());
    }

    @Override
    public String toString() {
        return orderColumns.toString();
    }

    @Override
    public void getVariables(Collection<LogicalVariable> variables) {
        variables.addAll(getColumns());
    }

    @Override
    public int hashCode() {
        return orderColumns.hashCode();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof LocalOrderProperty) {
            LocalOrderProperty lop = (LocalOrderProperty) object;
            return orderColumns.equals(lop.orderColumns);
        } else {
            return false;
        }
    }

    @Override
    public ILocalStructuralProperty normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        List<OrderColumn> normalizedOrderColumns = normalizeOrderingColumns(orderColumns, equivalenceClasses);
        reduceOrderingColumns(normalizedOrderColumns, fds);
        return new LocalOrderProperty(normalizedOrderColumns);
    }

    /**
     * Whether current property implies the required property
     *
     * @param required
     *            , a required property
     * @return true if the current property satisfies the required property; otherwise false.
     */
    public boolean implies(ILocalStructuralProperty required) {
        if (required.getPropertyType() != PropertyType.LOCAL_ORDER_PROPERTY) {
            return false;
        }
        LocalOrderProperty requiredOrderProperty = (LocalOrderProperty) required;
        Iterator<OrderColumn> requiredColumnIterator = requiredOrderProperty.getOrderColumns().iterator();
        Iterator<OrderColumn> currentColumnIterator = orderColumns.iterator();

        // Returns true if requiredColumnIterator is a prefix of currentColumnIterator.
        return PropertiesUtil.isPrefixOf(requiredColumnIterator, currentColumnIterator);
    }

    // Gets normalized  ordering columns, where each column variable is a representative variable of its equivalence
    // class, therefore, the matching of properties will can consider equivalence classes.
    private List<OrderColumn> normalizeOrderingColumns(List<OrderColumn> inputOrderColumns,
            Map<LogicalVariable, EquivalenceClass> equivalenceClasses) {
        List<OrderColumn> newOrderColumns = new ArrayList<>();
        if (equivalenceClasses == null || equivalenceClasses.isEmpty()) {
            newOrderColumns.addAll(inputOrderColumns);
            return newOrderColumns;
        }
        for (OrderColumn oc : inputOrderColumns) {
            LogicalVariable v = oc.getColumn();
            EquivalenceClass ec = equivalenceClasses.get(v);
            if (ec == null) {
                newOrderColumns.add(new OrderColumn(v, oc.getOrder()));
            } else {
                if (ec.representativeIsConst()) {
                    // trivially satisfied, so the var. can be removed
                } else {
                    newOrderColumns.add(new OrderColumn(ec.getVariableRepresentative(), oc.getOrder()));
                }
            }
        }
        return newOrderColumns;
    }

    // Using functional dependencies to eliminate unnecessary ordering columns.
    private void reduceOrderingColumns(List<OrderColumn> inputOrderColumns, List<FunctionalDependency> fds) {
        if (fds == null || fds.isEmpty()) {
            return;
        }
        Set<OrderColumn> impliedColumns = new HashSet<>();
        Set<LogicalVariable> currentPrefix = new HashSet<>();
        for (OrderColumn orderColumn : inputOrderColumns) {
            LogicalVariable orderVariable = orderColumn.getColumn();
            for (FunctionalDependency fdep : fds) {
                // Checks if the current ordering variable is implied by the prefix order columns.
                if (currentPrefix.containsAll(fdep.getHead()) && fdep.getTail().contains(orderVariable)) {
                    impliedColumns.add(orderColumn);
                    break;
                }
            }
            currentPrefix.add(orderVariable);
        }
        inputOrderColumns.removeAll(impliedColumns);
    }

    @Override
    public ILocalStructuralProperty retainVariables(Collection<LogicalVariable> vars) {
        List<LogicalVariable> columns = getColumns();
        List<LogicalVariable> newVars = new ArrayList<>();
        newVars.addAll(vars);
        newVars.retainAll(columns);
        List<OrderColumn> newColumns = new ArrayList<>();
        for (OrderColumn oc : orderColumns) {
            if (newVars.contains(oc.getColumn())) {
                newColumns.add(oc);
            } else {
                break;
            }
        }
        if (!newColumns.isEmpty()) {
            return new LocalOrderProperty(newColumns);
        } else {
            return null;
        }
    }

    @Override
    public ILocalStructuralProperty regardToGroup(Collection<LogicalVariable> groupKeys) {
        List<OrderColumn> newColumns = new ArrayList<>();
        for (OrderColumn oc : orderColumns) {
            if (!groupKeys.contains(oc.getColumn())) {
                newColumns.add(oc);
            }
        }
        if (!newColumns.isEmpty()) {
            return new LocalOrderProperty(newColumns);
        } else {
            return null;
        }
    }
}
