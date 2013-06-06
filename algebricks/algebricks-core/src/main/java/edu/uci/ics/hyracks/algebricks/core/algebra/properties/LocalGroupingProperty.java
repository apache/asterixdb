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
import java.util.List;
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class LocalGroupingProperty extends AbstractGroupingProperty implements ILocalStructuralProperty {

    // preferredOrderEnforcer, if not null, is guaranteed to enforce grouping on
    // columnSet
    private List<LogicalVariable> preferredOrderEnforcer;

    public LocalGroupingProperty(Set<LogicalVariable> columnSet) {
        super(columnSet);
    }

    public LocalGroupingProperty(Set<LogicalVariable> columnSet, List<LogicalVariable> preferredOrderEnforcer) {
        this(columnSet);
        this.preferredOrderEnforcer = preferredOrderEnforcer;
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
}
