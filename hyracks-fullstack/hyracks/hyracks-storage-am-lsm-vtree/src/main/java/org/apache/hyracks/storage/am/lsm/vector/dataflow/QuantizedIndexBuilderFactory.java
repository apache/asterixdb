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
package org.apache.hyracks.storage.am.lsm.vector.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;
import org.apache.hyracks.storage.am.common.api.IIndexBuilder;
import org.apache.hyracks.storage.am.common.build.IndexBuilderFactory;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Produces {@link QuantizedIndexBuilder}s for the quantized vector-index create path. The build ingredients
 * are identical to {@link IndexBuilderFactory}; only the concrete builder type differs, so the quantization
 * constants sampled upstream can be injected into the resource at build time.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public class QuantizedIndexBuilderFactory extends IndexBuilderFactory {

    private static final long serialVersionUID = 1L;

    public QuantizedIndexBuilderFactory(IStorageManager storageManager, IFileSplitProvider fileSplitProvider,
            IResourceFactory localResourceFactory, boolean durable) {
        super(storageManager, fileSplitProvider, localResourceFactory, durable);
    }

    @Override
    public IIndexBuilder create(IHyracksTaskContext ctx, int partition) throws HyracksDataException {
        FileReference resourceRef = fileSplitProvider.getFileSplits()[partition].getFileReference(ctx.getIoManager());
        return new QuantizedIndexBuilder(ctx.getJobletContext().getServiceContext(), storageManager,
                storageManager.getResourceIdFactory(ctx.getJobletContext().getServiceContext()), resourceRef,
                localResourceFactory, durable);
    }
}
