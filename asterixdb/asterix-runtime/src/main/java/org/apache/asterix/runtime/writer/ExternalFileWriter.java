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
package org.apache.asterix.runtime.writer;

import org.apache.hyracks.algebricks.runtime.writers.IExternalWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

final class ExternalFileWriter implements IExternalWriter {
    static final String UNRESOLVABLE_PATH = "UNRESOLVABLE_PATH";
    private final IPathResolver pathResolver;
    private final IExternalFileWriter writer;
    private final int maxResultPerFile;
    private String partitionPath;
    private int tupleCounter;

    public ExternalFileWriter(IPathResolver pathResolver, IExternalFileWriter writer, int maxResultPerFile) {
        this.pathResolver = pathResolver;
        this.writer = writer;
        this.maxResultPerFile = maxResultPerFile;
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void initNewPartition(IFrameTupleReference tuple) throws HyracksDataException {
        partitionPath = pathResolver.getPartitionDirectory(tuple);
        if (UNRESOLVABLE_PATH != partitionPath) {
            writer.validate(partitionPath);
            newFile();
        }
    }

    @Override
    public void write(IValueReference value) throws HyracksDataException {
        if (UNRESOLVABLE_PATH == partitionPath) {
            // Ignore writing values for unresolvable partition paths
            return;
        }

        // create a new file only when we reach the maximum tuples and we know a new tuple is incoming
        // e.g., if max is 1000, we hit tuple 1001, we will upload and create a new file, if we only have 1000
        // we will stop here, and calling the close/finish will upload whatever is written. This is to avoid
        // creating and uploading empty files
        if (tupleCounter >= maxResultPerFile) {
            newFile();
        }

        writer.write(value);
        tupleCounter++;
    }

    @Override
    public void abort() throws HyracksDataException {
        writer.abort();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    private void newFile() throws HyracksDataException {
        tupleCounter = 0;
        if (!writer.newFile(partitionPath, pathResolver.getNextFileName())) {
            // the partitionPath could contain illegal chars or the length of the total path is too long
            partitionPath = UNRESOLVABLE_PATH;
        }
    }
}
