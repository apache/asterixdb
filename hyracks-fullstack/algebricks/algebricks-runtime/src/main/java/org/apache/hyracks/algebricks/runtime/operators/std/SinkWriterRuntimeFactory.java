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
package org.apache.hyracks.algebricks.runtime.operators.std;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hyracks.algebricks.data.IAWriter;
import org.apache.hyracks.algebricks.data.IAWriterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractPushRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SinkWriterRuntimeFactory extends AbstractPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final int[] fields;
    private final IPrinterFactory[] printerFactories;
    private final File outputFile;
    private final RecordDescriptor inputRecordDesc;
    private final IAWriterFactory writerFactory;

    public SinkWriterRuntimeFactory(int[] fields, IPrinterFactory[] printerFactories, File outputFile,
            IAWriterFactory writerFactory, RecordDescriptor inputRecordDesc) {
        this.fields = fields;
        this.printerFactories = printerFactories;
        this.outputFile = outputFile;
        this.writerFactory = writerFactory;
        this.inputRecordDesc = inputRecordDesc;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("sink-write " + "[");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                buf.append("; ");
            }
            buf.append(fields[i]);
        }
        buf.append("] outputFile");
        return buf.toString();
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        try {
            PrintStream filePrintStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(outputFile)));
            IAWriter w = writerFactory.createWriter(fields, filePrintStream, printerFactories, inputRecordDesc);
            return new IPushRuntime[] { new SinkWriterRuntime(w, filePrintStream, inputRecordDesc, true) };
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
