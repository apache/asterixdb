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
import java.util.Set;

import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class LocalGroupingProperty extends AbstractGroupingProperty implements ILocalStructuralProperty {

    // preferredOrderEnforcer, which is guaranteed to enforce grouping on columnSet
    private final List<LogicalVariable> preferredOrderEnforcer = new ArrayList<>();

    public LocalGroupingProperty(Set<LogicalVariable> columnSet) {
        super(columnSet);
    }

    public LocalGroupingProperty(Set<LogicalVariable> columnSet, List<LogicalVariable> preferredOrderEnforcer) {
        this(columnSet);
        if (preferredOrderEnforcer != null) {
            this.preferredOrderEnforcer.addAll(preferredOrderEnforcer);
        }
    }

    @Override
    public PropertyType getPropertyType() {
        return PropertyType.LOCAL_GROUPING_PROPERTY;
    }

    @Override
    public void getColumns(Collection<LogicalVariable> columns) {
        columns.addAll(columnSet);
    }

    @Override
    public String toString() {
        return columnSet.toString();
    }

    @Override
    public void getVariables(Collection<LogicalVariable> variables) {
        variables.addAll(columnSet);
    }

    public List<LogicalVariable> getPreferredOrderEnforcer() {
        return preferredOrderEnforcer;
    }

    @Override
    public ILocalStructuralProperty retainVariables(Collection<LogicalVariable> vars) {
        Set<LogicalVariable> newVars = new ListSet<>();
        newVars.addAll(vars);
        newVars.retainAll(columnSet);
        if (columnSet.equals(newVars)) {
            return new LocalGroupingProperty(columnSet, preferredOrderEnforcer);
        }
        // Column set for the retained grouping property
        Set<LogicalVariable> newColumns = new ListSet<>();
        // Matches the prefix of the original column set.
        for (LogicalVariable v : columnSet) {
            if (newVars.contains(v)) {
                newColumns.add(v);
            } else {
                break;
            }
        }
        return createNewLocalGroupingProperty(newColumns);
    }

    @Override
    public ILocalStructuralProperty regardToGroup(Collection<LogicalVariable> groupKeys) {
        Set<LogicalVariable> newColumns = new ListSet<>();
        for (LogicalVariable v : columnSet) {
            if (!groupKeys.contains(v)) {
                newColumns.add(v);
            }
        }
        return createNewLocalGroupingProperty(newColumns);
    }

    @Override
    public ILocalStructuralProperty normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        Set<LogicalVariable> normalizedColumnSet =
                normalizeAndReduceGroupingColumns(columnSet, equivalenceClasses, fds);
        return new LocalGroupingProperty(normalizedColumnSet, preferredOrderEnforcer);
    }

    // Creates a new local grouping property from a new column set.
    private LocalGroupingProperty createNewLocalGroupingProperty(Set<LogicalVariable> newColumns) {
        if (newColumns.isEmpty()) {
            return null;
        }
        int numColumns = newColumns.size();
        List<LogicalVariable> newOrderEnforcer = preferredOrderEnforcer.size() > numColumns
                ? preferredOrderEnforcer.subList(0, numColumns) : preferredOrderEnforcer;
        return new LocalGroupingProperty(newColumns, newOrderEnforcer);
    }
}
