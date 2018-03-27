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

package org.apache.hyracks.dataflow.std.file;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class FileRemoveOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private final IFileSplitProvider fileSplitProvider;
    private final boolean quietly;

    public FileRemoveOperatorDescriptor(IOperatorDescriptorRegistry spec, IFileSplitProvider fileSplitProvder,
            boolean quietly) {
        super(spec, 0, 0);
        this.fileSplitProvider = fileSplitProvder;
        this.quietly = quietly;
    }

    /**
     *
     * @deprecated use {@link #FileRemoveOperatorDescriptor(IOperatorDescriptorRegistry spec, IFileSplitProvider fileSplitProvder, boolean quietly)} instead.
     */
    @Deprecated
    public FileRemoveOperatorDescriptor(IOperatorDescriptorRegistry spec, IFileSplitProvider fileSplitProvder) {
        this(spec, fileSplitProvder, false);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final FileSplit split = fileSplitProvider.getFileSplits()[partition];
        final IIOManager ioManager = ctx.getIoManager();
        return new AbstractOperatorNodePushable() {

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                throw new IllegalStateException();
            }

            @Override
            public void initialize() throws HyracksDataException {
                // will only work for files inside the io devices
                File f = split.getFile(ioManager);
                if (quietly) {
                    FileUtils.deleteQuietly(f);
                } else {
                    try {
                        FileUtils.deleteDirectory(f);
                    } catch (IOException e) {
                        throw HyracksDataException.create(e);
                    }
                }
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                throw new IllegalStateException();
            }

            @Override
            public int getInputArity() {
                return 0;
            }

            @Override
            public void deinitialize() throws HyracksDataException {
            }
        };
    }

}
