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
import java.io.Serializable;

import edu.uci.ics.hyracks.api.io.FileReference;

public class FileSplit implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String nodeName;
    private final FileReference file;
    private final int ioDeviceId;

    public FileSplit(String nodeName, FileReference file) {
        this.nodeName = nodeName;
        this.file = file;
        this.ioDeviceId = 0;
    }

    public FileSplit(String nodeName, FileReference file, int ioDeviceId) {
        this.nodeName = nodeName;
        this.file = file;
        this.ioDeviceId = ioDeviceId;
    }

    public FileSplit(String nodeName, String path) {
        this.nodeName = nodeName;
        this.file = new FileReference(new File(path));
        this.ioDeviceId = 0;
    }

    public String getNodeName() {
        return nodeName;
    }

    public FileReference getLocalFile() {
        return file;
    }

    public int getIODeviceId() {
        return ioDeviceId;
    }
}