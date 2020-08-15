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
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public final class PartialBroadcastOrderedIntersectProperty implements IPartitioningProperty {

    private final List<IntervalColumn> intervalColumns;

    private final RangeMap rangeMap;

    private INodeDomain domain;

    public PartialBroadcastOrderedIntersectProperty(List<IntervalColumn> intervalColumns, INodeDomain domain,
            RangeMap rangeMap) {
        this.intervalColumns = intervalColumns;
        this.domain = domain;
        this.rangeMap = rangeMap;
    }

    @Override
    public PartitioningType getPartitioningType() {
        return PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
    }

    @Override
    public String toString() {
        return getPartitioningType().toString() + intervalColumns + " domain:" + domain
                + (rangeMap != null ? " range-map:" + rangeMap : "");
    }

    public List<IntervalColumn> getIntervalColumns() {
        return intervalColumns;
    }

    @Override
    public void getColumns(Collection<LogicalVariable> columns) {
        for (IntervalColumn ic : intervalColumns) {
            columns.add(ic.getStartColumn());
            columns.add(ic.getEndColumn());
        }
    }

    public RangeMap getRangeMap() {
        return rangeMap;
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
    public IPartitioningProperty normalize(Map<LogicalVariable, EquivalenceClass> equivalenceClasses,
            List<FunctionalDependency> fds) {
        return this;
    }

    @Override
    public IPartitioningProperty substituteColumnVars(Map<LogicalVariable, LogicalVariable> varMap) {
        boolean applied = false;
        List<IntervalColumn> newIntervalColumns = new ArrayList<>(intervalColumns.size());
        for (IntervalColumn intervalColumn : intervalColumns) {
            LogicalVariable startColumnVar = intervalColumn.getStartColumn();
            LogicalVariable newStartColumnVar = varMap.get(startColumnVar);
            if (newStartColumnVar != null) {
                applied = true;
            } else {
                newStartColumnVar = startColumnVar;
            }

            LogicalVariable endColumnVar = intervalColumn.getEndColumn();
            LogicalVariable newEndColumnVar = varMap.get(endColumnVar);
            if (newEndColumnVar != null) {
                applied = true;
            } else {
                newEndColumnVar = endColumnVar;
            }

            newIntervalColumns.add(new IntervalColumn(newStartColumnVar, newEndColumnVar, intervalColumn.getOrder()));
        }
        return applied ? new PartialBroadcastOrderedIntersectProperty(newIntervalColumns, domain, rangeMap) : this;
    }

    @Override
    public IPartitioningProperty clonePartitioningProperty() {
        return new PartialBroadcastOrderedIntersectProperty(new ArrayList<>(intervalColumns), domain, rangeMap);
    }
}
