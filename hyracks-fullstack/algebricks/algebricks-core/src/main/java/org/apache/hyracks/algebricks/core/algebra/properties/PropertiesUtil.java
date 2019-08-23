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
import org.apache.hyracks.algebricks.core.algebra.properties.ILocalStructuralProperty.PropertyType;

public class PropertiesUtil {

    public Set<LogicalVariable> closureUnderFDs(Collection<LogicalVariable> vars, List<FunctionalDependency> fdList) {
        Set<LogicalVariable> k = new ListSet<>(vars);
        boolean change;
        do {
            change = false;
            for (FunctionalDependency fd : fdList) {
                List<LogicalVariable> h = fd.getHead();
                if (!k.containsAll(h)) {
                    continue;
                }
                List<LogicalVariable> t = fd.getTail();
                for (LogicalVariable v : t) {
                    if (!(k.contains(v))) {
                        k.add(v);
                        change = true;
                    }
                }
            }
        } while (change);
        return k;
    }

    /**
     * Checks whether delivered properties can satisfy required properties, considering equivalence class and
     * functional dependencies.
     *
     * @param reqd,
     *            the required property list.
     * @param dlvd,
     *            the delivered property list.
     * @param equivalenceClasses,
     *            a map from variables to their equivalence classes.
     * @param fds,
     *            a list of functional dependencies.
     * @return true if the delivered property list can satisfy the required property list;
     *         false otherwise.
     */
    public static boolean matchLocalProperties(List<ILocalStructuralProperty> reqd, List<ILocalStructuralProperty> dlvd,
            Map<LogicalVariable, EquivalenceClass> equivalenceClasses, List<FunctionalDependency> fds) {
        if (reqd == null) {
            return true;
        }
        if (dlvd == null) {
            return false;
        }
        return matchNormalizedLocalProperties(normalizeLocals(reqd, equivalenceClasses, fds),
                normalizeLocals(dlvd, equivalenceClasses, fds));
    }

    // Checks whether normalized delivered properties can satisfy normalized required property.
    private static boolean matchNormalizedLocalProperties(List<ILocalStructuralProperty> reqs,
            List<ILocalStructuralProperty> dlvds) {
        boolean hasOrderPropertyInReq = false;
        boolean hasGroupingPropertyInReq = false;
        boolean orderPropertyMet = false;
        boolean groupingPropertyMet = false;
        for (ILocalStructuralProperty req : reqs) {
            PropertyType reqType = req.getPropertyType();
            hasOrderPropertyInReq |= reqType == PropertyType.LOCAL_ORDER_PROPERTY;
            hasGroupingPropertyInReq |= reqType == PropertyType.LOCAL_GROUPING_PROPERTY;
            for (ILocalStructuralProperty dlvd : dlvds) {
                PropertyType dlvdType = dlvd.getPropertyType();
                if (reqType == PropertyType.LOCAL_ORDER_PROPERTY && dlvdType != PropertyType.LOCAL_ORDER_PROPERTY) {
                    // A grouping property cannot meet an order property, but an order property can meet a grouping
                    // property.
                    continue;
                }
                if (reqType == PropertyType.LOCAL_ORDER_PROPERTY) {
                    LocalOrderProperty lop = (LocalOrderProperty) dlvd;
                    // It is enough that one required ordering property is met.
                    orderPropertyMet |= lop.implies(req);
                } else {
                    Set<LogicalVariable> reqdCols = new ListSet<>();
                    Set<LogicalVariable> dlvdCols = new ListSet<>();
                    req.getColumns(reqdCols);
                    dlvd.getColumns(dlvdCols);
                    // It is enough that one required grouping property is met.
                    groupingPropertyMet |= isPrefixOf(reqdCols.iterator(), dlvdCols.iterator());
                }
            }
        }
        // Delivered properties satisfy required properties if one of required order properties is
        // satisfied and one of required grouping properties is satisfied.
        return (!hasOrderPropertyInReq || orderPropertyMet) && (!hasGroupingPropertyInReq || groupingPropertyMet);
    }

    public static boolean matchPartitioningProps(IPartitioningProperty reqd, IPartitioningProperty dlvd,
            boolean mayExpandProperties) {
        INodeDomain dom1 = reqd.getNodeDomain();
        INodeDomain dom2 = dlvd.getNodeDomain();
        if (!dom1.sameAs(dom2)) {
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
                            return (!ud.getColumnSet().isEmpty() && ur.getColumnSet().containsAll(ud.getColumnSet()));
                        } else {
                            return (ud.getColumnSet().equals(ur.getColumnSet()));
                        }
                    }
                    /*
                    //TODO: revisit this once we start reasoning about different partitioning functions (hash vs range)
                    case ORDERED_PARTITIONED: {
                        UnorderedPartitionedProperty ur = (UnorderedPartitionedProperty) reqd;
                        OrderedPartitionedProperty od = (OrderedPartitionedProperty) dlvd;
                        List<LogicalVariable> dlvdSortColumns = orderColumnsToVariables(od.getOrderColumns());
                        if (mayExpandProperties) {
                            return isPrefixOf(dlvdSortColumns.iterator(), ur.getColumnSet().iterator());
                        } else {
                            return ur.getColumnSet().containsAll(dlvdSortColumns)
                                    && dlvdSortColumns.containsAll(ur.getColumnSet());
                        }
                    }
                    */
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
                        //TODO: support non-null range maps
                        if (or.getRangeMap() != null || od.getRangeMap() != null) {
                            return false;
                        }
                        if (mayExpandProperties) {
                            return isPrefixOf(od.getOrderColumns().iterator(), or.getOrderColumns().iterator());
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
     * Converts a list of OrderColumns to a list of LogicalVariables.
     *
     * @param orderColumns
     *            , a list of OrderColumns
     * @return the list of LogicalVariables
     */
    private static List<LogicalVariable> orderColumnsToVariables(List<OrderColumn> orderColumns) {
        List<LogicalVariable> columns = new ArrayList<>();
        for (OrderColumn oc : orderColumns) {
            columns.add(oc.getColumn());
        }
        return columns;
    }

    /**
     * @param pref
     * @param target
     * @return true iff pref is a prefix of target
     */
    public static <T> boolean isPrefixOf(Iterator<T> pref, Iterator<T> target) {
        while (pref.hasNext()) {
            T v = pref.next();
            if (!target.hasNext()) {
                return false;
            }
            if (!v.equals(target.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Normalizes or reduces the order columns argument based on the functional dependencies argument. The caller is
     * responsible for taking caution as to how to handle the returned object since this method either returns the same
     * object that is passed or returns a new object.
     * @param orderColumns the order columns that are to be normalized
     * @param functionalDependencies {@link FunctionalDependency}
     * @return a new normalized object if normalization is applied. Otherwise, the same argument object is returned.
     */
    public static List<OrderColumn> applyFDsToOrderColumns(List<OrderColumn> orderColumns,
            List<FunctionalDependency> functionalDependencies) {
        if (functionalDependencies == null || functionalDependencies.isEmpty()) {
            return orderColumns;
        }

        // the set of vars. is ordered
        // so we try the variables in order from last to first
        int deleted = 0;
        boolean[] removedColumns = new boolean[orderColumns.size()];
        for (int i = orderColumns.size() - 1; i >= 0; i--) {
            for (FunctionalDependency functionalDependency : functionalDependencies) {
                if (impliedByPrefix(orderColumns, i, functionalDependency)) {
                    removedColumns[i] = true;
                    deleted++;
                    break;
                }
            }
        }
        List<OrderColumn> normalizedColumns = new ArrayList<>(orderColumns.size() - deleted);
        for (int i = 0; i < orderColumns.size(); i++) {
            if (!removedColumns[i]) {
                normalizedColumns.add(orderColumns.get(i));
            }
        }

        return normalizedColumns;
    }

    /**
     * Normalizes or reduces the order columns argument based on the equivalenceClasses argument. The caller is
     * responsible for taking caution as to how to handle the returned object since this method either returns the same
     * object that is passed or returns a new object.
     * @param orderColumns the order columns that are to be normalized
     * @param equivalenceClasses {@link EquivalenceClass}
     * @return a new normalized object if normalization is applied. Otherwise, the same argument object is returned.
     */
    public static List<OrderColumn> replaceOrderColumnsByEqClasses(List<OrderColumn> orderColumns,
            Map<LogicalVariable, EquivalenceClass> equivalenceClasses) {
        if (equivalenceClasses == null || equivalenceClasses.isEmpty()) {
            return orderColumns;
        }
        List<OrderColumn> norm = new ArrayList<>();
        for (OrderColumn orderColumn : orderColumns) {
            EquivalenceClass columnEQClass = equivalenceClasses.get(orderColumn.getColumn());
            if (columnEQClass == null) {
                norm.add(orderColumn);
            } else if (!columnEQClass.representativeIsConst()) {
                norm.add(new OrderColumn(columnEQClass.getVariableRepresentative(), orderColumn.getOrder()));
            }
            // else columnEQClass rep. is constant, i.e. trivially satisfied, so the var. can be removed
        }
        return norm;
    }

    private static boolean impliedByPrefix(List<OrderColumn> vars, int i, FunctionalDependency fdep) {
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

    // Gets normalized local structural properties according to equivalence classes and functional dependencies.
    private static List<ILocalStructuralProperty> normalizeLocals(List<ILocalStructuralProperty> props,
            Map<LogicalVariable, EquivalenceClass> equivalenceClasses, List<FunctionalDependency> fds) {
        List<ILocalStructuralProperty> normalizedLocalProperties = new ArrayList<>();
        for (ILocalStructuralProperty prop : props) {
            normalizedLocalProperties.add(prop.normalize(equivalenceClasses, fds));
        }
        return normalizedLocalProperties;
    }
}
