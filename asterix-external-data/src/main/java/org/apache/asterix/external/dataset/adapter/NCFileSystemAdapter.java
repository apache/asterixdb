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
package org.apache.asterix.external.dataset.adapter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Factory class for creating an instance of NCFileSystemAdapter. An
 * NCFileSystemAdapter reads external data residing on the local file system of
 * an NC.
 */
public class NCFileSystemAdapter extends FileSystemBasedAdapter {

    private static final long serialVersionUID = 1L;

    private final FileSplit[] fileSplits;

    public NCFileSystemAdapter(FileSplit[] fileSplits, ITupleParserFactory parserFactory, IAType atype,
            IHyracksTaskContext ctx) throws HyracksDataException {
        super(parserFactory, atype, ctx);
        this.fileSplits = fileSplits;
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        FileSplit split = fileSplits[partition];
        File inputFile = split.getLocalFile().getFile();
        InputStream in;
        try {
            in = new FileInputStream(inputFile);
            return in;
        } catch (FileNotFoundException e) {
            throw new IOException(e);
        }
    }

   
    @Override
    public String getFilename(int partition) {
        final FileSplit fileSplit = fileSplits[partition];
        return fileSplit.getNodeName() + ":" + fileSplit.getLocalFile().getFile().getPath();
    }

}
