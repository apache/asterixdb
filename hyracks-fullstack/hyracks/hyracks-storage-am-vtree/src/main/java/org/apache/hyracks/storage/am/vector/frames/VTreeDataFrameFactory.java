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

import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.storage.am.vector.api.IVTreeDataFrame;

/**
 * Factory for {@link VTreeDataFrame} instances. Data frames hold tuples sorted by
 * {@code distance_to_centroid} ascending; the exact tuple shape depends on whether the index is
 * quantized:
 * <ul>
 *   <li>Non-quantized: {@code <distance_to_centroid, centroid_id, primary_key, included_fields>}</li>
 *   <li>Quantized:     {@code <distance_to_centroid, centroid_id, quantized_distance,
 *       quantized_embedding, primary_key, included_fields>} (the default in this build, since
 *       quantization is enforced at index creation; pkStartField=4 vs 2)</li>
 * </ul>
 */
public class VTreeDataFrameFactory implements ITreeIndexFrameFactory {

    private static final long serialVersionUID = 1L;
    private final ITreeIndexTupleWriterFactory tupleWriterFactory;
    private final int vectorDimensions;

    public VTreeDataFrameFactory(ITreeIndexTupleWriterFactory tupleWriterFactory, int vectorDimensions) {
        this.tupleWriterFactory = tupleWriterFactory;
        this.vectorDimensions = vectorDimensions;
    }

    @Override
    public IVTreeDataFrame createFrame() {
        return new VTreeDataFrame(tupleWriterFactory.createTupleWriter());
    }

    @Override
    public ITreeIndexTupleWriterFactory getTupleWriterFactory() {
        return tupleWriterFactory;
    }

    public int getVectorDimensions() {
        return vectorDimensions;
    }
}
