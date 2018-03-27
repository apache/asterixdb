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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class FileScanOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final IFileSplitProvider fileSplitProvider;

    private final ITupleParserFactory tupleParserFactory;

    public FileScanOperatorDescriptor(IOperatorDescriptorRegistry spec, IFileSplitProvider fileSplitProvider,
            ITupleParserFactory tupleParserFactory, RecordDescriptor rDesc) {
        super(spec, 0, 1);
        this.fileSplitProvider = fileSplitProvider;
        this.tupleParserFactory = tupleParserFactory;
        outRecDescs[0] = rDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final FileSplit split = fileSplitProvider.getFileSplits()[partition];
        final ITupleParser tp = tupleParserFactory.createTupleParser(ctx);
        final IIOManager ioManager = ctx.getIoManager();
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                File f = split.getFile(ioManager);
                try {
                    writer.open();
                    InputStream in;
                    try {
                        in = new FileInputStream(f);
                    } catch (FileNotFoundException e) {
                        writer.fail();
                        throw HyracksDataException.create(e);
                    }
                    tp.parse(in, writer);
                } catch (Throwable th) {
                    writer.fail();
                    throw HyracksDataException.create(th);
                } finally {
                    writer.close();
                }
            }
        };
    }
}
