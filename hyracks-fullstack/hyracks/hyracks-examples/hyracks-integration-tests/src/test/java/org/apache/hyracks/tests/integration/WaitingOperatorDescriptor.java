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
package org.apache.hyracks.tests.integration;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

class WaitingOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    public static final MutableBoolean CONTINUE_RUNNING = new MutableBoolean(false);

    private static final long serialVersionUID = 1L;

    public WaitingOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity, int outputArity) {
        super(spec, inputArity, outputArity);
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        try {
            return new IOperatorNodePushable() {
                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                        throws HyracksDataException {
                }

                @Override
                public void initialize() throws HyracksDataException {
                    synchronized (CONTINUE_RUNNING) {
                        while (!CONTINUE_RUNNING.booleanValue()) {
                            try {
                                CONTINUE_RUNNING.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }

                @Override
                public IFrameWriter getInputFrameWriter(int index) {
                    return null;
                }

                @Override
                public int getInputArity() {
                    return inputArity;
                }

                @Override
                public String getDisplayName() {
                    return WaitingOperatorDescriptor.class.getSimpleName() + ".OperatorNodePushable:" + partition;
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                }
            };
        } finally {
        }
    }
}
