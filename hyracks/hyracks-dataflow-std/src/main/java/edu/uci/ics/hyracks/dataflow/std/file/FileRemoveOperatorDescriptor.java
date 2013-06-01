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

package edu.uci.ics.hyracks.dataflow.std.file;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class FileRemoveOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private final IFileSplitProvider fileSplitProvider;

    public FileRemoveOperatorDescriptor(IOperatorDescriptorRegistry spec, IFileSplitProvider fileSplitProvder) {
        super(spec, 0, 0);
        this.fileSplitProvider = fileSplitProvder;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final FileSplit split = fileSplitProvider.getFileSplits()[partition];
        return new AbstractOperatorNodePushable() {

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
                throw new IllegalStateException();
            }

            @Override
            public void initialize() throws HyracksDataException {
                File f = split.getLocalFile().getFile();
                try {
                    FileUtils.deleteDirectory(f);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
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
