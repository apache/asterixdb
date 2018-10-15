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
package org.apache.hyracks.dataflow.common.data.partition;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class RepartitionComputerFactory implements ITuplePartitionComputerFactory {
    private static final long serialVersionUID = 1L;

    private int factor;
    private ITuplePartitionComputerFactory delegateFactory;

    public RepartitionComputerFactory(int factor, ITuplePartitionComputerFactory delegate) {
        this.factor = factor;
        this.delegateFactory = delegate;
    }

    @Override
    public ITuplePartitionComputer createPartitioner(IHyracksTaskContext hyracksTaskContext) {
        return new ITuplePartitionComputer() {
            private ITuplePartitionComputer delegate = delegateFactory.createPartitioner(hyracksTaskContext);

            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                return delegate.partition(accessor, tIndex, factor * nParts) / factor;
            }
        };
    }
}
