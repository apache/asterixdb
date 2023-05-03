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
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class FileRemoveOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 2L;

    private final IFileSplitProvider fileSplitProvider;
    private final boolean quietly;
    private final int[][] partitionsMap;

    public FileRemoveOperatorDescriptor(IOperatorDescriptorRegistry spec, IFileSplitProvider fileSplitProvder,
            boolean quietly, int[][] partitionsMap) {
        super(spec, 0, 0);
        this.fileSplitProvider = fileSplitProvder;
        this.quietly = quietly;
        this.partitionsMap = partitionsMap;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final FileSplit[] splits = fileSplitProvider.getFileSplits();
        final int[] splitsIndexes = partitionsMap[partition];
        final IIOManager ioManager = ctx.getIoManager();
        return new AbstractOperatorNodePushable() {

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                throw new IllegalStateException();
            }

            @Override
            public void initialize() throws HyracksDataException {
                // will only work for files inside the io devices
                deleteFiles();
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

            private void deleteFiles() throws HyracksDataException {
                Throwable failure = null;
                for (int splitsIndex : splitsIndexes) {
                    try {
                        File file = splits[splitsIndex].getFile(ioManager);
                        if (quietly) {
                            FileUtils.deleteQuietly(file);
                        } else {
                            FileUtils.deleteDirectory(file);
                        }
                    } catch (Throwable th) {
                        failure = ExceptionUtils.suppress(failure, th);
                    }
                }
                if (failure != null) {
                    throw HyracksDataException.create(failure);
                }
            }
        };
    }

}
