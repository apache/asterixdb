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
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.utils.VTreeStaticTupleAccessor;

/**
 * Factory for {@link VTreeInteriorFrame} instances. {@code centroidDimensions} is retained for
 * future per-factory dimension validation.
 *
 * <p>Interior tuples are NOT caller-parameterized — every VTree interior frame uses the same
 * fixed three-field layout {@code <cluster_id, centroid, child_page_pointer>}. The schema and the
 * tuple writer it implies are owned by this factory so call sites need only supply the dimension
 * and null-handling knobs.
 */
public class VTreeInteriorFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private final ITreeIndexTupleWriter tupleWriter;
    private final int centroidDimensions;

    public VTreeInteriorFrameFactory(int centroidDimensions, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        this.centroidDimensions = centroidDimensions;
        this.tupleWriter = new TypeAwareTupleWriterFactory(VTreeStaticTupleAccessor.interiorTypeTraits(),
                nullTypeTraits, nullIntrospector).createTupleWriter();
    }

    @Override
    public IVTreeInteriorFrame createFrame() {
        return new VTreeInteriorFrame(tupleWriter);
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
