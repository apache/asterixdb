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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

public final class UnorderedPartitionedProperty extends AbstractGroupingProperty implements IPartitioningProperty {

    private INodeDomain domain;
    private final int[][] partitionsMap;

    private UnorderedPartitionedProperty(Set<LogicalVariable> partitioningVariables, INodeDomain domain,
            int[][] partitionsMap) {
        super(partitioningVariables);
        this.domain = domain;
        this.partitionsMap = partitionsMap;
    }

    public static UnorderedPartitionedProperty of(Set<LogicalVariable> partitioningVariables, INodeDomain domain) {
        return new UnorderedPartitionedProperty(partitioningVariables, domain, null);
    }

    public static UnorderedPartitionedProperty ofPartitionsMap(Set<LogicalVariable> partitioningVariables,
            INodeDomain domain, int[][] partitionsMap) {
        return new UnorderedPartitionedProperty(partitioningVariables, domain, partitionsMap);
    }

    @Override
    public PartitioningType getPartitioningType() {
        return PartitioningType.UNORDERED_PARTITIONED;
    }

    @Override
    public IPartitioningProperty normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        Set<LogicalVariable> normalizedColumnSet =
                normalizeAndReduceGroupingColumns(columnSet, equivalenceClasses, fds);
        return new UnorderedPartitionedProperty(normalizedColumnSet, domain, partitionsMap);
    }

    @Override
    public String toString() {
        return getPartitioningType().toString() + columnSet + " domain:" + domain;
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

    @Override
    public IPartitioningProperty substituteColumnVars(Map<LogicalVariable, LogicalVariable> varMap) {
        boolean applied = false;
        Set<LogicalVariable> newColumnSet = new ListSet<>();
        for (LogicalVariable variable : columnSet) {
            if (varMap.containsKey(variable)) {
                newColumnSet.add(varMap.get(variable));
                applied = true;
            } else {
                newColumnSet.add(variable);
            }
        }
        return applied ? new UnorderedPartitionedProperty(newColumnSet, domain, partitionsMap) : this;
    }

    @Override
    public IPartitioningProperty clonePartitioningProperty() {
        return new UnorderedPartitionedProperty(new ListSet<>(columnSet), domain, partitionsMap);
    }

    public int[][] getPartitionsMap() {
        return partitionsMap;
    }

    public boolean usesPartitionsMap() {
        return partitionsMap != null;
    }

    public boolean samePartitioningScheme(UnorderedPartitionedProperty another) {
        return Arrays.deepEquals(partitionsMap, another.partitionsMap);
    }
}
