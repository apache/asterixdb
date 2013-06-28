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

public final class FileReference implements Serializable {
    private static final long serialVersionUID = 1L;

    private final File file;
    private final IODeviceHandle dev;

    public FileReference(IODeviceHandle dev, String devRelPath) {
        file = new File(dev.getPath(), devRelPath);
        this.dev = dev;
    }

    public FileReference(File file) {
        this.file = file;
        this.dev = null;
    }

    public File getFile() {
    	return file;
    }

    public IODeviceHandle getDeviceHandle() {
    	return dev;
    }
    
    @Override
    public String toString() {
        return file.getAbsolutePath();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FileReference)) {
            return false;
        }
        return file.equals(((FileReference) o).file);
    }

    @Override
    public int hashCode() {
        return file.hashCode();
    }

    public void delete() {
        file.delete();
    }
}