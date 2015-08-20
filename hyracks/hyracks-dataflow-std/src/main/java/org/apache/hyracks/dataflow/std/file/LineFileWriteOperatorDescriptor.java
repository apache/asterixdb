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
import java.io.FileOutputStream;
import java.io.OutputStream;

import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;

public class LineFileWriteOperatorDescriptor extends AbstractFileWriteOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static class LineWriterImpl extends RecordWriter {
        LineWriterImpl(File file, int[] columns, char separator) throws Exception {
            super(columns, separator, new Object[] { file });
        }

        private static final long serialVersionUID = 1L;

        @Override
        public OutputStream createOutputStream(Object[] args) throws Exception {
            return new FileOutputStream((File) args[0]);
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

    public LineFileWriteOperatorDescriptor(IOperatorDescriptorRegistry spec, FileSplit[] splits, int[] columns, char separator) {
        super(spec, splits);
        this.columns = columns;
        this.separator = separator;
    }

    @Override
    protected IRecordWriter createRecordWriter(FileSplit fileSplit, int index) throws Exception {
        return new LineWriterImpl(fileSplit.getLocalFile().getFile(), columns, separator);
    }
}