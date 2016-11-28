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

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A FileSplit that is mapped to a specific IO device
 */
public class MappedFileSplit extends ManagedFileSplit {

    private static final long serialVersionUID = 1L;
    private final int ioDevice;
    private transient FileReference cached;

    /**
     * Construct a managed File split that is mapped to an IO device
     * @param node
     * @param path
     * @param ioDevice
     */
    public MappedFileSplit(String node, String path, int ioDevice) {
        super(node, path);
        this.ioDevice = ioDevice;
    }

    public int getIoDevice() {
        return ioDevice;
    }

    @Override
    public String toString() {
        return "Node: " + getNodeName() + " IO Device: " + ioDevice + " managed path: " + getPath();
    }

    @Override
    public File getFile(IIOManager ioManager) throws HyracksDataException {
        return getFileReference(ioManager).getFile();
    }

    @Override
    public FileReference getFileReference(IIOManager ioManager) throws HyracksDataException {
        if (cached == null) {
            cached = new FileReference(ioManager.getIODevices().get(ioDevice), getPath());
        }
        return cached;
    }
}
