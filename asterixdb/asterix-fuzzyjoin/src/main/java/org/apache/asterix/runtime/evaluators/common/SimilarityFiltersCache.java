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
package org.apache.asterix.runtime.evaluators.common;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.asterix.fuzzyjoin.similarity.SimilarityFilters;
import org.apache.asterix.fuzzyjoin.similarity.SimilarityFiltersFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class SimilarityFiltersCache {
    private final UTF8StringSerializerDeserializer utf8SerDer = new UTF8StringSerializerDeserializer();

    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream dis = new DataInputStream(bbis);

    private float similarityThresholdCached = 0;
    private byte[] similarityNameBytesCached = null;
    private SimilarityFilters similarityFiltersCached = null;

    public SimilarityFilters get(float similarityThreshold, byte[] similarityNameBytes, int startOffset, int len)
            throws HyracksDataException {
        if (similarityThreshold != similarityThresholdCached || similarityNameBytesCached == null
                || !Arrays.equals(similarityNameBytes, similarityNameBytesCached)) {
            bbis.setByteBuffer(ByteBuffer.wrap(similarityNameBytes), startOffset + 1);
            String similarityName = utf8SerDer.deserialize(dis);
            similarityNameBytesCached = Arrays.copyOfRange(similarityNameBytes, startOffset, len);
            similarityFiltersCached =
                    SimilarityFiltersFactory.getSimilarityFilters(similarityName, similarityThreshold);
        }
        return similarityFiltersCached;
    }
}
