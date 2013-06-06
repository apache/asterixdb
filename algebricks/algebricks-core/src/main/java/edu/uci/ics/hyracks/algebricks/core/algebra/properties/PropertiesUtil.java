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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.common.utils.ListSet;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;

public class PropertiesUtil {

    public Set<LogicalVariable> closureUnderFDs(Collection<LogicalVariable> vars, List<FunctionalDependency> fdList) {
        Set<LogicalVariable> k = new ListSet<LogicalVariable>(vars);
        boolean change;
        do {
            change = false;
            for (FunctionalDependency fd : fdList) {
                List<LogicalVariable> h = fd.getHead();
                if (k.containsAll(h)) {
                    List<LogicalVariable> t = fd.getTail();
                    for (LogicalVariable v : t) {
                        if (!(k.contains(v))) {
                            k.add(v);
                            change = true;
                        }
                    }
                }
            }
        } while (change);
        return k;
    }

    public static boolean matchLocalProperties(List<ILocalStructuralProperty> reqd,
            List<ILocalStructuralProperty> dlvd, Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        if (reqd == null) {
            return true;
        }
        if (dlvd == null) {
            return false;
        }
        normalizeLocals(reqd, equivalenceClasses, fds);
        normalizeLocals(dlvd, equivalenceClasses, fds);

        ListIterator<ILocalStructuralProperty> dlvdIter = dlvd.listIterator();

        Set<LogicalVariable> rqdCols = new ListSet<LogicalVariable>();
        Set<LogicalVariable> dlvdCols = new ListSet<LogicalVariable>();
        for (ILocalStructuralProperty r : reqd) {
            if (r.getPropertyType() == PropertyType.LOCAL_GROUPING_PROPERTY) {
                rqdCols.clear();
                r.getVariables(rqdCols);
            }
            boolean implied = false;
            while (!implied && dlvdIter.hasNext()) {
                ILocalStructuralProperty d = dlvdIter.next();
                switch (r.getPropertyType()) {
                    case LOCAL_ORDER_PROPERTY: {
                        if (d.getPropertyType() != PropertyType.LOCAL_ORDER_PROPERTY) {
                            return false;
                        }
                        LocalOrderProperty lop = (LocalOrderProperty) d;
                        if (lop.getColumn() == ((LocalOrderProperty) r).getColumn()
                                && lop.getOrder() == ((LocalOrderProperty) r).getOrder()) {
                            implied = true;
                        } else {
                            return false;
                        }
                        break;
                    }
                    case LOCAL_GROUPING_PROPERTY: {
                        dlvdCols.clear();
                        d.getColumns(dlvdCols);
                        for (LogicalVariable v : dlvdCols) {
                            if (rqdCols.contains(v)) {
                                rqdCols.remove(v);
                            } else {
                                return false;
                            }
                        }
                        if (rqdCols.isEmpty()) {
                            implied = true;
                        }
                        break;
                    }
                    default: {
                        throw new IllegalStateException();
                    }
                }
            }
            if (!implied) {
                return false;
            }
        }
        return true;

    }

    public static boolean matchPartitioningProps(IPartitioningProperty reqd, IPartitioningProperty dlvd,
            boolean mayExpandProperties) {

        INodeDomain dom1 = reqd.getNodeDomain();
        INodeDomain dom2 = dlvd.getNodeDomain();
        if (dom1 != null && dom2 != null && !dom1.sameAs(dom2)) {
            return false;
        }

        switch (reqd.getPartitioningType()) {
            case RANDOM: {
                // anything matches RANDOM
                return true;
            }
            case UNORDERED_PARTITIONED: {
                switch (dlvd.getPartitioningType()) {
                    case UNORDERED_PARTITIONED: {
                        UnorderedPartitionedProperty ur = (UnorderedPartitionedProperty) reqd;
                        UnorderedPartitionedProperty ud = (UnorderedPartitionedProperty) dlvd;
                        if (mayExpandProperties) {
                            return ur.getColumnSet().containsAll(ud.getColumnSet());
                        } else {
                            return ur.getColumnSet().equals(ud.getColumnSet());
                        }
                    }
                    case ORDERED_PARTITIONED: {
                        UnorderedPartitionedProperty ur = (UnorderedPartitionedProperty) reqd;
                        OrderedPartitionedProperty od = (OrderedPartitionedProperty) dlvd;
                        if (mayExpandProperties) {
                            return ur.getColumnSet().containsAll(od.getOrderColumns());
                        } else {
                            return ur.getColumnSet().containsAll(od.getOrderColumns())
                                    && od.getOrderColumns().containsAll(ur.getColumnSet());
                        }
                    }
                    default: {
                        return false;
                    }
                }
            }
            case ORDERED_PARTITIONED: {
                switch (dlvd.getPartitioningType()) {
                    case ORDERED_PARTITIONED: {
                        OrderedPartitionedProperty or = (OrderedPartitionedProperty) reqd;
                        OrderedPartitionedProperty od = (OrderedPartitionedProperty) dlvd;
                        if (mayExpandProperties) {
                            return isPrefixOf(od.getOrderColumns(), or.getOrderColumns());
                        } else {
                            return od.getOrderColumns().equals(or.getOrderColumns());
                        }
                    }
                    default: {
                        return false;
                    }
                }
            }
            default: {
                return (dlvd.getPartitioningType() == reqd.getPartitioningType());
            }
        }
    }

    /**
     * @param pref
     * @param target
     * @return true iff pref is a prefix of target
     */
    private static boolean isPrefixOf(List<OrderColumn> pref, List<OrderColumn> target) {
        Iterator<OrderColumn> iter = target.iterator();
        for (OrderColumn v : pref) {
            if (!iter.hasNext()) {
                return false;
            }
            if (!v.equals(iter.next())) {
                return false;
            }
        }
        return true;
    }

    public static ArrayList<OrderColumn> applyFDsToOrderColumns(ArrayList<OrderColumn> orderColumns,
            List<FunctionalDependency> fds) {
        // the set of vars. is ordered
        // so we try the variables in order from last to first
        if (fds == null || fds.isEmpty()) {
            return orderColumns;
        }

        int deleted = 0;
        for (int i = orderColumns.size() - 1; i >= 0; i--) {
            for (FunctionalDependency fdep : fds) {
                if (impliedByPrefix(orderColumns, i, fdep)) {
                    orderColumns.set(i, null);
                    deleted++;
                    break;
                }
            }
        }
        ArrayList<OrderColumn> norm = new ArrayList<OrderColumn>(orderColumns.size() - deleted);
        for (OrderColumn oc : orderColumns) {
            if (oc != null) {
                norm.add(oc);
            }
        }
        return norm;
    }

    public static ArrayList<OrderColumn> replaceOrderColumnsByEqClasses(ArrayList<OrderColumn> orderColumns,
            Map<LogicalVariable, EquivalenceClass> equivalenceClasses) {
        if (equivalenceClasses == null || equivalenceClasses.isEmpty()) {
            return orderColumns;
        }
        ArrayList<OrderColumn> norm = new ArrayList<OrderColumn>();
        for (OrderColumn v : orderColumns) {
            EquivalenceClass ec = equivalenceClasses.get(v.getColumn());
            if (ec == null) {
                norm.add(v);
            } else {
                if (ec.representativeIsConst()) {
                    // trivially satisfied, so the var. can be removed
                } else {
                    norm.add(new OrderColumn(ec.getVariableRepresentative(), v.getOrder()));
                }
            }
        }
        return norm;
    }

    private static boolean impliedByPrefix(ArrayList<OrderColumn> vars, int i, FunctionalDependency fdep) {
        if (!fdep.getTail().contains(vars.get(i).getColumn())) {
            return false;
        }
        boolean fdSat = true;
        for (LogicalVariable pv : fdep.getHead()) {
            boolean isInPrefix = false;
            for (int j = 0; j < i; j++) {
                if (vars.get(j).getColumn().equals(pv)) {
                    isInPrefix = true;
                    break;
                }
            }
            if (!isInPrefix) {
                fdSat = false;
                break;
            }
        }
        return fdSat;
    }

    private static void normalizeLocals(List<ILocalStructuralProperty> props,
            Map<LogicalVariable, EquivalenceClass> equivalenceClasses, List<FunctionalDependency> fds) {
        ListIterator<ILocalStructuralProperty> propIter = props.listIterator();
        int pos = -1;
        while (propIter.hasNext()) {
            ILocalStructuralProperty p = propIter.next();
            if (p.getPropertyType() == PropertyType.LOCAL_GROUPING_PROPERTY) {
                ((LocalGroupingProperty) p).normalizeGroupingColumns(equivalenceClasses, fds);
                pos++;
            } else {
                LocalOrderProperty ord = (LocalOrderProperty) p;
                EquivalenceClass ec = equivalenceClasses.get(ord.getColumn());
                if (ec != null) {
                    if (ec.representativeIsConst()) {
                        propIter.remove();
                    } else {
                        ord.getOrderColumn().setColumn(ec.getVariableRepresentative());
                        pos++;
                    }
                } else {
                    pos++;
                }
            }
        }

        if (pos < 1) {
            return;
        }

        while (propIter.hasPrevious()) {
            ILocalStructuralProperty p = propIter.previous();
            ListIterator<ILocalStructuralProperty> secondIter = props.listIterator(pos);
            pos--;
            Set<LogicalVariable> cols = new ListSet<LogicalVariable>();
            while (secondIter.hasPrevious()) {
                secondIter.previous().getColumns(cols);
            }
            secondIter = null;
            for (FunctionalDependency fdep : fds) {
                LinkedList<LogicalVariable> columnsOfP = new LinkedList<LogicalVariable>();
                p.getColumns(columnsOfP);
                if (impliedByPrefix(columnsOfP, cols, fdep)) {
                    propIter.remove();
                    break;
                }
            }
        }
    }

    private static boolean impliedByPrefix(List<LogicalVariable> colsOfProp, Set<LogicalVariable> colsOfPrefix,
            FunctionalDependency fdep) {
        return fdep.getTail().containsAll(colsOfProp) && colsOfPrefix.containsAll(fdep.getHead());
    }
}
