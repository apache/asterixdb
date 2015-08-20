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
import java.util.Map;
import java.util.Set;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public final class UnorderedPartitionedProperty extends AbstractGroupingProperty implements IPartitioningProperty {

    private INodeDomain domain;

    public UnorderedPartitionedProperty(Set<LogicalVariable> partitioningVariables, INodeDomain domain) {
        super(partitioningVariables);
        this.domain = domain;
    }

    @Override
    public PartitioningType getPartitioningType() {
        return PartitioningType.UNORDERED_PARTITIONED;
    }

    @Override
    public void normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses, List<FunctionalDependency> fds) {
        normalizeGroupingColumns(equivalenceClasses, fds);
    }

    @Override
    public String toString() {
        return getPartitioningType().toString() + columnSet;
    }

    @Override
    public void getColumns(Collection<LogicalVariable> columns) {
        columns.addAll(columnSet);
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
