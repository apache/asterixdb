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
package edu.uci.ics.hyracks.api.io;

import java.io.File;
import java.io.Serializable;

public class IODeviceHandle implements Serializable {
    private static final long serialVersionUID = 1L;

    private final File path;

    private final String workAreaPath;

    public IODeviceHandle(File path, String workAreaPath) {
        this.path = path;
        workAreaPath = workAreaPath.trim();
        if (workAreaPath.endsWith(File.separator)) {
            workAreaPath = workAreaPath.substring(0, workAreaPath.length() - 1);
        }
        this.workAreaPath = workAreaPath;
    }

    public File getPath() {
        return path;
    }

    public String getWorkAreaPath() {
        return workAreaPath;
    }

    public FileReference createFileReference(String relPath) {
        return new FileReference(this, relPath);
    }
}