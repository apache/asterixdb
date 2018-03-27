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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExceptionOnCreatePushRuntimeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private static Logger LOGGER = LogManager.getLogger();
    private static AtomicInteger createPushRuntime = new AtomicInteger();
    private static AtomicInteger initializeCounter = new AtomicInteger();
    private static AtomicInteger openCloseCounter = new AtomicInteger();
    public static final String ERROR_MESSAGE = "I throw exceptions";
    private final int[] exceptionPartitions;
    private final boolean sleepOnInitialize;

    public ExceptionOnCreatePushRuntimeOperatorDescriptor(IOperatorDescriptorRegistry spec, int inputArity,
            int outputArity, int[] exceptionPartitions, boolean sleepOnInitialize) {
        super(spec, inputArity, outputArity);
        this.exceptionPartitions = exceptionPartitions;
        this.sleepOnInitialize = sleepOnInitialize;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        createPushRuntime.incrementAndGet();
        try {
            if (exceptionPartitions != null) {
                for (int p : exceptionPartitions) {
                    if (p == partition) {
                        throw new HyracksDataException(ERROR_MESSAGE);
                    }
                }
            }
            return new IOperatorNodePushable() {
                @Override
                public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                        throws HyracksDataException {
                }

                @Override
                public void initialize() throws HyracksDataException {
                    initializeCounter.incrementAndGet();
                    if (sleepOnInitialize) {
                        try {
                            synchronized (this) {
                                wait();
                            }
                        } catch (InterruptedException e) {
                            // can safely interrupt thread since this is a task thread
                            Thread.currentThread().interrupt();
                            throw HyracksDataException.create(e);
                        }
                    }
                }

                @Override
                public IFrameWriter getInputFrameWriter(int index) {
                    return new IFrameWriter() {
                        @Override
                        public void open() throws HyracksDataException {
                            openCloseCounter.incrementAndGet();
                        }

                        @Override
                        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        }

                        @Override
                        public void fail() throws HyracksDataException {
                        }

                        @Override
                        public void close() throws HyracksDataException {
                            openCloseCounter.decrementAndGet();
                        }
                    };
                }

                @Override
                public int getInputArity() {
                    return inputArity;
                }

                @Override
                public String getDisplayName() {
                    return ExceptionOnCreatePushRuntimeOperatorDescriptor.class.getSimpleName()
                            + ".OperatorNodePushable:" + partition;
                }

                @Override
                public void deinitialize() throws HyracksDataException {
                    initializeCounter.decrementAndGet();
                }
            };
        } finally {
            createPushRuntime.decrementAndGet();
        }
    }

    public static boolean succeed() {
        boolean success = openCloseCounter.get() == 0 && createPushRuntime.get() == 0 && initializeCounter.get() == 0;
        if (!success) {
            LOGGER.log(Level.ERROR, "Failure:");
            LOGGER.log(Level.ERROR, "CreatePushRuntime:" + createPushRuntime.get());
            LOGGER.log(Level.ERROR, "InitializeCounter:" + initializeCounter.get());
            LOGGER.log(Level.ERROR, "OpenCloseCounter:" + openCloseCounter.get());
        }
        return success;
    }

    public static String stats() {
        return "Failure: CreatePushRuntime:" + createPushRuntime.get() + ", InitializeCounter:"
                + initializeCounter.get() + ", OpenCloseCounter:" + openCloseCounter.get();
    }
}
