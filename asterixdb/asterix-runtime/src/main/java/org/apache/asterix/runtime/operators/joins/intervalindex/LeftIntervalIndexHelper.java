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

package org.apache.asterix.runtime.operators.joins.intervalindex;

import java.util.Comparator;
import java.util.logging.Logger;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedDeletableTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;

public class LeftIntervalIndexHelper extends AbstractIntervalIndexHelper {

    private static final int MEMORY_PARTITION = 0;

    private static final Logger LOGGER = Logger.getLogger(LeftIntervalIndexHelper.class.getName());

    public LeftIntervalIndexHelper(IHyracksTaskContext ctx, RecordDescriptor rd, int key,
            IPartitionedDeletableTupleBufferManager bufferManager, Comparator<EndPointIndexItem> endPointComparator, byte point,
            int partition) {
        super(ctx, rd, key, bufferManager, endPointComparator, point, partition);
    }

//    @Override
//    protected int getMemoryPartition() {
//        return MEMORY_PARTITION;
//    }

}