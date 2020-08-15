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

package org.apache.asterix.optimizer.rules.util;

import java.util.Arrays;
import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty.PartitioningType;
import org.apache.hyracks.algebricks.core.algebra.properties.IntervalColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class IntervalPartitions {

    private final RangeMap rangeMap;
    private final List<IntervalColumn> leftIntervalColumn;
    private final List<IntervalColumn> rightIntervalColumn;
    private final PartitioningType leftPartitioningType;
    private final PartitioningType rightPartitioningType;

    IntervalPartitions(RangeMap rangeMap, List<IntervalColumn> leftIntervalColumn,
            List<IntervalColumn> rightIntervalColumn, PartitioningType leftPartitioningType,
            PartitioningType rightPartitioningType) {
        this.rangeMap = rangeMap;
        this.leftIntervalColumn = leftIntervalColumn;
        this.rightIntervalColumn = rightIntervalColumn;
        this.leftPartitioningType = leftPartitioningType;
        this.rightPartitioningType = rightPartitioningType;
    }

    public RangeMap getRangeMap() {
        return rangeMap;
    }

    public PartitioningType getLeftPartitioningType() {
        return leftPartitioningType;
    }

    public PartitioningType getRightPartitioningType() {
        return rightPartitioningType;
    }

    public List<IntervalColumn> getLeftIntervalColumn() {
        return leftIntervalColumn;
    }

    public List<IntervalColumn> getRightIntervalColumn() {
        return rightIntervalColumn;
    }

    public List<OrderColumn> getLeftStartColumn() {
        LogicalVariable leftStartLogicalVariable = leftIntervalColumn.get(0).getStartColumn();
        List<OrderColumn> leftOrderColumn =
                Arrays.asList(new OrderColumn(leftStartLogicalVariable, leftIntervalColumn.get(0).getOrder()));
        return leftOrderColumn;
    }

    public List<OrderColumn> getRightStartColumn() {
        LogicalVariable rightStartLogicalVariable = rightIntervalColumn.get(0).getStartColumn();
        List<OrderColumn> rightOrderColumn =
                Arrays.asList(new OrderColumn(rightStartLogicalVariable, rightIntervalColumn.get(0).getOrder()));
        return rightOrderColumn;
    }
}
