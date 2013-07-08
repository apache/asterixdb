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
package edu.uci.ics.hyracks.dataflow.common.data.partition;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class RepartitionComputerGeneratorFactory implements ITuplePartitionComputerFamily{
	
	 private static final long serialVersionUID = 1L;

	    private int factor;
	    private ITuplePartitionComputerFamily delegateFactory;

	    public RepartitionComputerGeneratorFactory(int factor, ITuplePartitionComputerFamily delegate) {
	        this.factor = factor;
	        this.delegateFactory = delegate;
	    }

	@Override
	public ITuplePartitionComputer createPartitioner(int seed) {
		final int s = seed;
		return new ITuplePartitionComputer() {
            private ITuplePartitionComputer delegate = delegateFactory.createPartitioner(s);

            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
            	return delegate.partition(accessor, tIndex, factor * nParts) / factor;
            }
        };
	}

}
