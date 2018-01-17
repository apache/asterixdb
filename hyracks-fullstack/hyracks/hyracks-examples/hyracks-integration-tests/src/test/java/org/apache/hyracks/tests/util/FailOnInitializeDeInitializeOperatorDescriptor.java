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

package org.apache.hyracks.tests.util;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class FailOnInitializeDeInitializeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    public static final String INIT_ERROR_MESSAGE = "Failure on initialize()";
    public static final String DEINIT_ERROR_MESSAGE = "Failure on deinitialize()";
    private final boolean failOnInit;
    private final boolean failOnDeinit;

    public FailOnInitializeDeInitializeOperatorDescriptor(IOperatorDescriptorRegistry spec, boolean failOnInit,
            boolean failOnDeInit) {
        super(spec, 0, 1);
        this.failOnInit = failOnInit;
        this.failOnDeinit = failOnDeInit;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new IOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                if (failOnInit) {
                    throw new RuntimeException(INIT_ERROR_MESSAGE);
                }
            }

            @Override
            public void deinitialize() throws HyracksDataException {
                if (failOnDeinit) {
                    throw new RuntimeException(DEINIT_ERROR_MESSAGE);
                }
            }

            @Override
            public int getInputArity() {
                return 0;
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                    throws HyracksDataException {
                // ignore
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                return null;
            }

            @Override
            public String getDisplayName() {
                return FailOnInitializeDeInitializeOperatorDescriptor.class.getSimpleName();
            }
        };
    }
}
