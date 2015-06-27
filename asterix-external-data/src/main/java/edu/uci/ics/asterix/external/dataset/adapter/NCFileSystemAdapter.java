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
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

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
