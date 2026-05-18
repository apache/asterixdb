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

package org.apache.hyracks.storage.am.vector.impls;

import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilder;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataTupleBuilderFactory;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;

/**
 * Factory for creating {@link VTreeDataTupleBuilder} instances.
 *
 * Carries the tuple layout context (numIncludeFields, isQuantized) determined
 * at index creation time. Quantization parameters are supplied at creator
 * construction time since they are loaded from LSMVTreeLocalResource metadata at index
 * activation, after this factory has been persisted.
 */
public class VTreeDataTupleBuilderFactory implements IVTreeDataTupleBuilderFactory {

    private static final long serialVersionUID = 2L;

    private final int numIncludeFields;
    private final boolean isQuantized;

    public VTreeDataTupleBuilderFactory(int numIncludeFields, boolean isQuantized) {
        this.numIncludeFields = numIncludeFields;
        this.isQuantized = isQuantized;
    }

    @Override
    public IVTreeDataTupleBuilder createDataTupleBuilder(VTreeQuantizationParams quantizationParams) {
        return new VTreeDataTupleBuilder(numIncludeFields, isQuantized, quantizationParams);
    }

    @Override
    public boolean isQuantized() {
        return isQuantized;
    }
}
