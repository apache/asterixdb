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

package org.apache.hyracks.storage.am.vector.api;

import java.io.Serializable;

import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Factory for {@link IVTreeDataTupleBuilder} instances.
 * <p>
 * {@link Serializable} so it can travel on {@code LSMVTreeLocalResourceFactory} in the Java-serialized
 * index-creation job. Not {@code IJsonSerializable}: {@code LSMVTreeLocalResource.fromJson} rebuilds it
 * from primitives ({@code numIncludeFields}, {@code isQuantized}) rather than serializing it as JSON.
 */
public interface IVTreeDataTupleBuilderFactory extends Serializable {

    /**
     * Creates a new data tuple creator.
     *
     * @param quantizationParams the scalar-quantization params, or {@code null} when the index is
     *        non-quantized, in which case the quantized fields are not written
     * @return a new IVTreeDataTupleBuilder
     */
    IVTreeDataTupleBuilder createDataTupleBuilder(VTreeQuantizationParams quantizationParams);

    /**
     * Whether the tuples produced by this factory use the quantized data-tuple layout
     * {@code [distance, centroidId, quantized_distance, quantized_embedding, PK..., includes...]}
     * (pkStartField=4) rather than the non-quantized {@code [distance, centroidId, PK...,
     * includes...]} layout (pkStartField=2).
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    boolean isQuantized();
}
