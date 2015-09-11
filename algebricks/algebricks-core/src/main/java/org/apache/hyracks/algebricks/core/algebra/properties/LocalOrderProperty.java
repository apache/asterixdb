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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.algebricks.common.utils.ListSet;
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
        List<LogicalVariable> orderVars = new ArrayList<LogicalVariable>();
        for (OrderColumn oc : orderColumns) {
            orderVars.add(oc.getColumn());
        }
        return orderVars;
    }

    public List<OrderKind> getOrders() {
        List<OrderKind> orderKinds = new ArrayList<OrderKind>();
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
    public boolean equals(Object object) {
        LocalOrderProperty lop = (LocalOrderProperty) object;
        return orderColumns.equals(lop.orderColumns);
    }

    /**
     * Whether current property implies the required property
     * 
     * @param required
     *            , a required property
     * @return true if the current property satisfies the required property; otherwise false.
     */
    public final boolean implies(ILocalStructuralProperty required) {
        if (required.getPropertyType() != PropertyType.LOCAL_ORDER_PROPERTY) {
            return false;
        }
        LocalOrderProperty requiredOrderProperty = (LocalOrderProperty) required;
        Iterator<OrderColumn> requiredColumnIterator = requiredOrderProperty.getOrderColumns().iterator();
        Iterator<OrderColumn> currentColumnIterator = orderColumns.iterator();

        // Returns true if requiredColumnIterator is a prefix of currentColumnIterator.
        return isPrefixOf(requiredColumnIterator, currentColumnIterator);
    }

    private <T> boolean isPrefixOf(Iterator<T> requiredColumnIterator, Iterator<T> currentColumnIterator) {
        while (requiredColumnIterator.hasNext()) {
            T oc = requiredColumnIterator.next();
            if (!currentColumnIterator.hasNext()) {
                return false;
            }
            if (!oc.equals(currentColumnIterator.next())) {
                return false;
            }
        }
        return true;
    }

    public final void normalizeOrderingColumns(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        replaceOrderingColumnsByEqClasses(equivalenceClasses);
        applyFDsToOrderingColumns(fds);
    }

    private void replaceOrderingColumnsByEqClasses(Map<LogicalVariable, EquivalenceClass> equivalenceClasses) {
        if (equivalenceClasses == null || equivalenceClasses.isEmpty()) {
            return;
        }
        List<OrderColumn> norm = new ArrayList<OrderColumn>();
        for (OrderColumn oc : orderColumns) {
            LogicalVariable v = oc.getColumn();
            EquivalenceClass ec = equivalenceClasses.get(v);
            if (ec == null) {
                norm.add(new OrderColumn(v, oc.getOrder()));
            } else {
                if (ec.representativeIsConst()) {
                    // trivially satisfied, so the var. can be removed
                } else {
                    norm.add(new OrderColumn(ec.getVariableRepresentative(), oc.getOrder()));
                }
            }
        }
        orderColumns = norm;
    }

    private void applyFDsToOrderingColumns(List<FunctionalDependency> fds) {
        if (fds == null || fds.isEmpty()) {
            return;
        }
        Set<LogicalVariable> norm = new ListSet<LogicalVariable>();
        List<LogicalVariable> columns = getColumns();
        for (LogicalVariable v : columns) {
            boolean isImpliedByAnFD = false;
            for (FunctionalDependency fdep : fds) {
                if (columns.containsAll(fdep.getHead()) && fdep.getTail().contains(v)) {
                    isImpliedByAnFD = true;
                    norm.addAll(fdep.getHead());
                    break;
                }

            }
            if (!isImpliedByAnFD) {
                norm.add(v);
            }
        }
        Set<OrderColumn> impliedColumns = new ListSet<OrderColumn>();
        for (OrderColumn oc : orderColumns) {
            if (!norm.contains(oc.getColumn())) {
                impliedColumns.add(oc);
            }
        }
        orderColumns.removeAll(impliedColumns);
    }

    @Override
    public ILocalStructuralProperty retainVariables(Collection<LogicalVariable> vars) {
        List<LogicalVariable> columns = getColumns();
        List<LogicalVariable> newVars = new ArrayList<LogicalVariable>();
        newVars.addAll(vars);
        newVars.retainAll(columns);
        List<OrderColumn> newColumns = new ArrayList<OrderColumn>();
        for (OrderColumn oc : orderColumns) {
            if (newVars.contains(oc.getColumn())) {
                newColumns.add(oc);
            } else {
                break;
            }
        }
        if (newColumns.size() > 0) {
            return new LocalOrderProperty(newColumns);
        } else {
            return null;
        }
    }

    @Override
    public ILocalStructuralProperty regardToGroup(Collection<LogicalVariable> groupKeys) {
        List<OrderColumn> newColumns = new ArrayList<OrderColumn>();
        for (OrderColumn oc : orderColumns) {
            if (!groupKeys.contains(oc.getColumn())) {
                newColumns.add(oc);
            }
        }
        if (newColumns.size() > 0) {
            return new LocalOrderProperty(newColumns);
        } else {
            return null;
        }
    }
}
