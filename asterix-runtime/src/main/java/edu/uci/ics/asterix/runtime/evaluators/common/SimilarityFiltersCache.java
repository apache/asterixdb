/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class SimilarityFiltersCache {

    private final ByteBufferInputStream bbis = new ByteBufferInputStream();
    private final DataInputStream dis = new DataInputStream(bbis);

    private float similarityThresholdCached = 0;
    private byte[] similarityNameBytesCached = null;
    private SimilarityFilters similarityFiltersCached = null;

    public SimilarityFilters get(float similarityThreshold, byte[] similarityNameBytes) throws AlgebricksException {
        if (similarityThreshold != similarityThresholdCached || similarityNameBytesCached == null
                || !Arrays.equals(similarityNameBytes, similarityNameBytesCached)) {
            bbis.setByteBuffer(ByteBuffer.wrap(similarityNameBytes), 1);
            String similarityName;
            try {
                similarityName = UTF8StringSerializerDeserializer.INSTANCE.deserialize(dis);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
            similarityNameBytesCached = Arrays.copyOf(similarityNameBytes, similarityNameBytes.length);
            similarityFiltersCached = SimilarityFiltersFactory
                    .getSimilarityFilters(similarityName, similarityThreshold);
        }
        return similarityFiltersCached;
    }
}
