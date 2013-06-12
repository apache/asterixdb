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
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

/**
 * Factory class for creating an instance of NCFileSystemAdapter. An
 * NCFileSystemAdapter reads external data residing on the local file system of
 * an NC.
 */
public class NCFileSystemAdapter extends FileSystemBasedAdapter {

    private static final long serialVersionUID = 1L;
    private FileSplit[] fileSplits;

    public NCFileSystemAdapter(IAType atype) {
        super(atype);
    }

    @Override
    public void configure(Map<String, Object> arguments) throws Exception {
        this.configuration = arguments;
        String[] splits = ((String) arguments.get(KEY_PATH)).split(",");
        configureFileSplits(splits);
        configureFormat();
    }

    @Override
    public void initialize(IHyracksTaskContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.READ;
    }

    private void configureFileSplits(String[] splits) throws AsterixException {
        if (fileSplits == null) {
            fileSplits = new FileSplit[splits.length];
            String nodeName;
            String nodeLocalPath;
            int count = 0;
            String trimmedValue;
            for (String splitPath : splits) {
                trimmedValue = splitPath.trim();
                if (!trimmedValue.contains("://")) {
                    throw new AsterixException("Invalid path: " + splitPath
                            + "\nUsage- path=\"Host://Absolute File Path\"");
                }
                nodeName = trimmedValue.split(":")[0];
                nodeLocalPath = trimmedValue.split("://")[1];
                FileSplit fileSplit = new FileSplit(nodeName, new FileReference(new File(nodeLocalPath)));
                fileSplits[count++] = fileSplit;
            }
        }
    }

    private void configurePartitionConstraint() throws AsterixException {
        String[] locs = new String[fileSplits.length];
        String location;
        for (int i = 0; i < fileSplits.length; i++) {
            location = getNodeResolver().resolveNode(fileSplits[i].getNodeName());
            locs[i] = location;
        }
        partitionConstraint = new AlgebricksAbsolutePartitionConstraint(locs);
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
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        if (partitionConstraint == null) {
            configurePartitionConstraint();
        }
        return partitionConstraint;
    }
}
