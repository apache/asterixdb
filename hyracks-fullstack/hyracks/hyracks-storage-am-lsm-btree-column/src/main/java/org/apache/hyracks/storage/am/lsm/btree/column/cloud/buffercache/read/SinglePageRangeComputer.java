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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Merge the given ranges such that the maximum number of ranges <= N.
 * Merge should be greedy as the range having lower gaps should be given priority.
 */
final class SinglePageRangeComputer extends AbstractPageRangesComputer {
    int rangeStart;
    int rangeEnd;
    private int numberOfRanges;

    @Override
    int getMaxNumberOfRanges() {
        return 1;
    }

    @Override
    void clear() {
        numberOfRanges = 0;
        requestedPages.clear();
    }

    @Override
    void addRange(int start, int end) {
        if (numberOfRanges++ == 0) {
            rangeStart = start;
        }
        rangeEnd = end;
        requestedPages.set(start, end + 1);
    }

    @Override
    void pin(CloudMegaPageReadContext ctx, IBufferCache bufferCache, int fileId, int pageZeroId)
            throws HyracksDataException {
        int numberOfPages = rangeEnd - rangeStart + 1;
        ctx.pin(bufferCache, fileId, pageZeroId, rangeStart, numberOfPages, requestedPages);
    }
}
