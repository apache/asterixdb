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
package org.apache.hyracks.dataflow.std.join;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangePartitionType.RangePartitioningType;

public class NaturalMergeJoinCheckerFactory implements IMergeJoinCheckerFactory {
    private static final long serialVersionUID = 1L;
    private IBinaryComparatorFactory[] comparatorFactories;

    public NaturalMergeJoinCheckerFactory(IBinaryComparatorFactory[] comparatorFactories) {
        this.comparatorFactories = comparatorFactories;
    }

    @Override
    public IMergeJoinChecker createMergeJoinChecker(int[] keys0, int[] keys1, int partition) {
        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        FrameTuplePairComparator ftp = new FrameTuplePairComparator(keys0, keys1, comparators);
        return new NaturalMergeJoinChecker(ftp);
    }

    public RangePartitioningType getLeftPartitioningType() {
        return RangePartitioningType.PROJECT;
    }

    public RangePartitioningType getRightPartitioningType() {
        return RangePartitioningType.PROJECT;
    }

}