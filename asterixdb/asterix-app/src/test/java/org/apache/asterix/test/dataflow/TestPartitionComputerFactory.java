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
package org.apache.asterix.test.dataflow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class TestPartitionComputerFactory implements ITuplePartitionComputerFactory {

    private static final long serialVersionUID = 1L;
    private final List<Integer> destinations;

    /*
     * For test purposes, this partition computer produces partitions according to a passed in
     * list of integers.
     */
    public TestPartitionComputerFactory(List<Integer> destinations) {
        this.destinations = destinations;
    }

    @Override
    public ITuplePartitionComputer createPartitioner(IHyracksTaskContext ctx) {
        return new ITuplePartitionComputer() {
            private final List<Integer> destinations =
                    new ArrayList<Integer>(TestPartitionComputerFactory.this.destinations);
            private Iterator<Integer> iterator = destinations.iterator();

            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                if (destinations.size() == 0) {
                    return 0;
                }
                while (!iterator.hasNext()) {
                    iterator = destinations.iterator();
                }
                return iterator.next();
            }
        };
    }
}
