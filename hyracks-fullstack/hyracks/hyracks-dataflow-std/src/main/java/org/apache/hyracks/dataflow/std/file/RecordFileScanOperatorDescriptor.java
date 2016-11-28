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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;

public class RecordFileScanOperatorDescriptor extends AbstractDeserializedFileScanOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    public RecordFileScanOperatorDescriptor(IOperatorDescriptorRegistry spec, FileSplit[] splits,
            RecordDescriptor recordDescriptor) {
        super(spec, splits, recordDescriptor);
    }

    private static class RecordReaderImpl implements IRecordReader {
        private RecordDescriptor recordDesc;
        private DataInputStream in;

        RecordReaderImpl(File file, RecordDescriptor recordDesc) throws Exception {
            this.in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
            this.recordDesc = recordDesc;
        }

        @Override
        public boolean read(Object[] record) throws Exception {
            in.mark(1);
            if (in.read() < 0) {
                return false;
            }
            in.reset();
            for (int i = 0; i < record.length; ++i) {
                record[i] = recordDesc.getFields()[i].deserialize(in);
            }
            return true;
        }

        @Override
        public void close() {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected IRecordReader createRecordReader(File file, RecordDescriptor desc) throws Exception {
        return new RecordReaderImpl(file, desc);
    }

    @Override
    protected void configure() throws Exception {
        // currently a no-op, but is meant to initialize , if required before it
        // is asked
        // to create a record reader
        // this is executed at the node and is useful for operators that could
        // not be
        // initialized from the client completely, because of lack of
        // information specific
        // to the node where the operator gets executed.

    }
}
