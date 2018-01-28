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
import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Represents an IO device
 */
public class IODeviceHandle implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * Absolute mount point
     */
    private final File mount;
    /**
     * relative workspace
     */
    private final String workspace;

    /**
     * @param mount
     *            The device root
     * @param workspace
     *            The relative workspace inside the device
     */
    public IODeviceHandle(File mount, String workspace) {
        this.mount = mount;
        this.workspace = workspace == null ? null
                : workspace.endsWith(File.separator) ? workspace.substring(0, workspace.length() - 1) : workspace;
    }

    public File getMount() {
        return mount;
    }

    public String getWorkspace() {
        return workspace;
    }

    /**
     * Create a file reference
     *
     * @param relPath
     *            the relative path
     * @return
     */
    public FileReference createFileRef(String relPath) {
        return new FileReference(this, relPath);
    }

    /**
     * Get handles for IO devices
     *
     * @param ioDevices
     *            comma separated list of devices
     * @return
     */
    public static List<IODeviceHandle> getDevices(String[] ioDevices) {
        List<IODeviceHandle> devices = new ArrayList<>();
        for (String ioDevice : ioDevices) {
            String devPath = ioDevice.trim();
            devices.add(new IODeviceHandle(new File(devPath), "."));
        }
        return devices;
    }

    /**
     * @param absolutePath
     * @return the relative path
     * @throws HyracksDataException
     */
    public String getRelativePath(String absolutePath) throws HyracksDataException {
        if (absolutePath.indexOf(mount.getAbsolutePath()) != 0) {
            throw new HyracksDataException(
                    "Passed path: " + absolutePath + " is not inside the device " + mount.getAbsolutePath());
        }
        return absolutePath.substring(mount.getAbsolutePath().length());
    }

    /**
     * determinea if the device contains a file with the passed relative path
     * @param relPath
     * @return true if it contains, false, otherwise
     */
    public boolean contains(String relPath) {
        return new File(mount, relPath).exists();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o instanceof IODeviceHandle) {
            return mount.getAbsolutePath().equals(((IODeviceHandle) o).getMount().getAbsolutePath());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return mount.getAbsolutePath().hashCode();
    }

    @Override
    public String toString() {
        return "mount: " + mount.getAbsolutePath() + ((workspace == null) ? "" : ", workspace: " + workspace);
    }
}
