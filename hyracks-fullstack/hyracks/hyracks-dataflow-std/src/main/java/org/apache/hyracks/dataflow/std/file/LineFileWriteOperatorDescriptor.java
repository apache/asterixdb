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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;

public class LineFileWriteOperatorDescriptor extends AbstractFileWriteOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static class LineWriterImpl extends RecordWriter {
        LineWriterImpl(File file, int[] columns, char separator) throws HyracksDataException {
            super(columns, separator, new Object[] { file });
        }

        @Override
        public OutputStream createOutputStream(Object[] args) throws HyracksDataException {
            try {
                return new FileOutputStream((File) args[0]);
            } catch (FileNotFoundException e) {
                throw HyracksDataException.create(e);
            }
        }
    }

    private int[] columns;
    private char separator;

    public LineFileWriteOperatorDescriptor(IOperatorDescriptorRegistry spec, FileSplit[] splits) {
        this(spec, splits, null, RecordWriter.COMMA);
    }

    public LineFileWriteOperatorDescriptor(IOperatorDescriptorRegistry spec, FileSplit[] splits, int[] columns) {
        this(spec, splits, columns, RecordWriter.COMMA);
    }

    public LineFileWriteOperatorDescriptor(IOperatorDescriptorRegistry spec, FileSplit[] splits, int[] columns,
            char separator) {
        super(spec, splits);
        this.columns = columns;
        this.separator = separator;
    }

    @Override
    protected IRecordWriter createRecordWriter(IIOManager ioManager, FileSplit fileSplit, int index)
            throws HyracksDataException {
        return new LineWriterImpl(fileSplit.getFile(ioManager), columns, separator);
    }
}
