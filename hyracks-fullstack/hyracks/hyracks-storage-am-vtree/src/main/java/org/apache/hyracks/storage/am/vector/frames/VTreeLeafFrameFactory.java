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

package org.apache.hyracks.storage.am.vector.frames;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeStaticTupleAccessor;

/**
 * Factory for {@link VTreeLeafFrame} instances. {@code centroidDimensions} is retained for
 * future per-factory dimension validation.
 *
 * <p>Leaf tuples are NOT caller-parameterized — the schema is determined entirely by whether
 * quantization is enabled:
 * <ul>
 *   <li>Quantized (production): {@code <cluster_id, centroid, quantized_embedding, child_page_pointer>}</li>
 *   <li>Non-quantized (test fixtures only): {@code <cluster_id, centroid, child_page_pointer>}</li>
 * </ul>
 */
public class VTreeLeafFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private final ITreeIndexTupleWriter tupleWriter;
    private final int centroidDimensions;

    public VTreeLeafFrameFactory(int centroidDimensions, boolean quantized, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        this.centroidDimensions = centroidDimensions;
        ITypeTraits[] schema = VTreeStaticTupleAccessor.leafTypeTraits(quantized);
        this.tupleWriter =
                new TypeAwareTupleWriterFactory(schema, nullTypeTraits, nullIntrospector).createTupleWriter();
    }

    @Override
    public IVTreeLeafFrame createFrame() {
        return new VTreeLeafFrame(tupleWriter);
    }

    @Override
    public ITreeIndexTupleWriterFactory getTupleWriterFactory() {
        return null;
    }

    public ITreeIndexTupleWriter getTupleWriter() {
        return tupleWriter;
    }

    public int getCentroidDimensions() {
        return centroidDimensions;
    }
}
