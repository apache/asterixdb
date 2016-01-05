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
package org.apache.asterix.external.input.stream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.std.file.FileSplit;

public class LocalFSInputStreamProvider implements IInputStreamProvider {

    private FileSplit[] fileSplits;
    private int partition;

    public LocalFSInputStreamProvider(FileSplit[] fileSplits, IHyracksTaskContext ctx,
            Map<String, String> configuration, int partition) {
        this.partition = partition;
        this.fileSplits = fileSplits;
    }

    @Override
    public AInputStream getInputStream() throws Exception {
        FileSplit split = fileSplits[partition];
        File inputFile = split.getLocalFile().getFile();
        InputStream in;
        try {
            in = new FileInputStream(inputFile);
            return new BasicInputStream(in);
        } catch (FileNotFoundException e) {
            throw new IOException(e);
        }
    }

}
