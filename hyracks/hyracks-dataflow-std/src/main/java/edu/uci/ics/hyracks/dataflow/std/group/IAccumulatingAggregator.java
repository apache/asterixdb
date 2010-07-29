/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.comm.io.FrameTupleAppender;

public interface IAccumulatingAggregator {
    /**
     * Called once per aggregator before calling accumulate for the first time.
     * 
     * @param accessor
     *            - Accessor to the data tuple.
     * @param tIndex
     *            - Index of the tuple in the accessor.
     * @throws HyracksDataException
     */
    public void init(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException;

    /**
     * Called once per tuple that belongs to this group.
     * 
     * @param accessor
     *            - Accessor to data tuple.
     * @param tIndex
     *            - Index of tuple in the accessor.
     * @throws HyracksDataException
     */
    public void accumulate(IFrameTupleAccessor accessor, int tIndex) throws HyracksDataException;

    /**
     * Called finally to emit output. This method is called until it returns true. The method is free to
     * write out output to the provided appender until there is no more space and return false. It is the
     * caller's responsibility to flush and make room in the appender before this method is called again.
     * 
     * @param appender
     *            - Appender to write output to.
     * @param accessor
     *            - Accessor to access the key.
     * @param tIndex
     *            - Tuple index of the key in the accessor.
     * @return true if all output is written, false if the appender is full.
     * @throws HyracksDataException
     */
    public boolean output(FrameTupleAppender appender, IFrameTupleAccessor accessor, int tIndex)
            throws HyracksDataException;
}