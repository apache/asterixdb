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

package org.apache.hyracks.storage.am.common.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.api.IIndexBuilderFactory;

public class IndexCreateOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 2L;
    private final IIndexBuilderFactory[][] indexBuilderFactories;
    private final int[][] partitionsMap;

    public IndexCreateOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexBuilderFactory[][] indexBuilderFactories, int[][] partitionsMap) {
        super(spec, 0, 0);
        this.indexBuilderFactories = indexBuilderFactories;
        this.partitionsMap = partitionsMap;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        int[] storagePartitions = partitionsMap[partition];
        IIndexBuilderFactory[] partitionIndexBuilderFactories = indexBuilderFactories[partition];
        IIndexBuilder[] indexBuilders = new IIndexBuilder[storagePartitions.length];
        for (int i = 0; i < storagePartitions.length; i++) {
            indexBuilders[i] = partitionIndexBuilderFactories[i].create(ctx, storagePartitions[i]);
        }
        return new IndexCreateOperatorNodePushable(indexBuilders);
    }
}
