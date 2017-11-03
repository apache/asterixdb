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

import java.util.EnumSet;
import java.util.Set;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class IndexDropOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    public enum DropOption {
        IF_EXISTS,
        WAIT_ON_IN_USE
    }

    private static final long serialVersionUID = 1L;
    private final IIndexDataflowHelperFactory dataflowHelperFactory;
    private final Set<DropOption> options;

    public IndexDropOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory dataflowHelperFactory) {
        this(spec, dataflowHelperFactory, EnumSet.noneOf(DropOption.class));
    }

    public IndexDropOperatorDescriptor(IOperatorDescriptorRegistry spec,
            IIndexDataflowHelperFactory dataflowHelperFactory, Set<DropOption> options) {
        super(spec, 0, 0);
        this.dataflowHelperFactory = dataflowHelperFactory;
        this.options = options;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new IndexDropOperatorNodePushable(dataflowHelperFactory, options, ctx, partition);
    }
}
