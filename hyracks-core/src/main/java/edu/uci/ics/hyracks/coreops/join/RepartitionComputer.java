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
package edu.uci.ics.hyracks.coreops.join;

import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.comm.io.FrameTupleAccessor;

public class RepartitionComputer implements ITuplePartitionComputer {
    private int factor;
    private ITuplePartitionComputer delegate;

    public RepartitionComputer(int factor, ITuplePartitionComputer delegate) {
        super();
        this.factor = factor;
        this.delegate = delegate;
    }

    @Override
    public int partition(FrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
        return delegate.partition(accessor, tIndex, factor * nParts) / factor;
    }
}