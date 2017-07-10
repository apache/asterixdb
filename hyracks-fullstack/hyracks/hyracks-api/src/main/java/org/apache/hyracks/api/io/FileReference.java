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
package org.apache.hyracks.api.io;

import java.io.File;
import java.io.Serializable;

/**
 * A device handle and a relative path.
 * Used to identify a file in the local Node Controller.
 * Only used for files which are stored inside an IO device.
 */
public final class FileReference implements Serializable {
    private static final long serialVersionUID = 1L;
    private final File file;
    private final IODeviceHandle dev;
    private final String path;

    public FileReference(IODeviceHandle dev, String path) {
        file = new File(dev.getMount(), path);
        this.dev = dev;
        this.path = path;
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
        return path.equals(((FileReference) o).path) && dev.equals(((FileReference) o).dev);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    /**
     * Delete the file
     *
     * @return true if file was deleted, false, otherwise
     */
    public boolean delete() {
        return file.delete();
    }

    /**
     * @return the relative path
     */
    public String getRelativePath() {
        return path;
    }

    /**
     * @return the absolute path
     */
    public String getAbsolutePath() {
        return file.getAbsolutePath();
    }

    public FileReference getChild(String name) {
        return new FileReference(dev, path + File.separator + name);
    }
}
