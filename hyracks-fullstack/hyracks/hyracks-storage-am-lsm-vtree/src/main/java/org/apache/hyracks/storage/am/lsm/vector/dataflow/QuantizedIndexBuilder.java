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

import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.storage.am.common.build.IndexBuilder;
import org.apache.hyracks.storage.am.vector.api.IQuantizedResource;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.common.IResource;
import org.apache.hyracks.storage.common.IResourceFactory;
import org.apache.hyracks.storage.common.IResourceWrapper;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.file.IResourceIdFactory;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Vector-index-specific {@link IndexBuilder} that carries scalar-quantization constants and injects them
 * into the freshly created resource at build time. Quantization is a vector concept, so the params and the
 * {@link IQuantizedResource} injection live here rather than on the shared {@link IndexBuilder}, which builds
 * every index type.
 * <p>
 * The constants are not known when the builder is constructed: they are sampled upstream and delivered to
 * {@link #setQuantizationParameters(VTreeQuantizationParams)} by
 * {@link QuantizedIndexCreateOperatorDescriptor} before
 * {@link #build()} runs.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public class QuantizedIndexBuilder extends IndexBuilder {

    private VTreeQuantizationParams quantizationParams;

    public QuantizedIndexBuilder(INCServiceContext ctx, IStorageManager storageManager,
            IResourceIdFactory resourceIdFactory, FileReference resourceRef, IResourceFactory localResourceFactory,
            boolean durable) throws HyracksDataException {
        super(ctx, storageManager, resourceIdFactory, resourceRef, localResourceFactory, durable);
    }

    public void setQuantizationParameters(VTreeQuantizationParams quantizationParams) {
        this.quantizationParams = quantizationParams;
    }

    @Override
    protected void configureResource(IResource resource) throws HyracksDataException {
        if (quantizationParams == null) {
            return;
        }
        // The builder receives the DatasetLocalResource wrapper; the quantizable resource is the one it
        // wraps. Unwrap generically (via IResourceWrapper) so the vector-only quantization concern stays
        // out of the shared DatasetLocalResource.
        IResource target =
                (resource instanceof IResourceWrapper) ? ((IResourceWrapper) resource).getResource() : resource;
        if (target instanceof IQuantizedResource) {
            ((IQuantizedResource) target).setQuantizationParameters(quantizationParams);
        }
    }
}
