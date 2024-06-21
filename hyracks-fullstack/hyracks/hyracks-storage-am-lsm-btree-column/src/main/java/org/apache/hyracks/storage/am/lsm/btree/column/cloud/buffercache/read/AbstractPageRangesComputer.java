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
package org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.read;

import java.util.BitSet;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public abstract class AbstractPageRangesComputer {
    protected static final int INITIAL_SIZE = 40;
    // Indicates a page is requested or not
    protected final BitSet requestedPages;

    AbstractPageRangesComputer() {
        requestedPages = new BitSet();
    }

    abstract int getMaxNumberOfRanges();

    /**
     * Clear ranges
     */
    abstract void clear();

    /**
     * Add a range
     *
     * @param start range start
     * @param end   range end
     */
    abstract void addRange(int start, int end);

    /**
     * Pin the calculated ranges
     *
     * @param ctx         Column mega-page buffer cache read context
     * @param bufferCache buffer cache
     * @param fileId      fileId
     * @param pageZeroId  page zero ID
     */
    abstract void pin(CloudMegaPageReadContext ctx, IBufferCache bufferCache, int fileId, int pageZeroId)
            throws HyracksDataException;

    /**
     * Creates a new range computer
     *
     * @param maxNumberOfRanges maximum number of ranges
     * @return a new instance of {@link AbstractPageRangesComputer}
     */
    static AbstractPageRangesComputer create(int maxNumberOfRanges) {
        if (maxNumberOfRanges == 1) {
            return new SinglePageRangeComputer();
        }

        return new PageRangesComputer(maxNumberOfRanges);
    }
}
