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
 * A node and a managed path inside an IO device.
 */
public class ManagedFileSplit extends FileSplit {
    private static final long serialVersionUID = 1L;

    /**
     * Constructor
     *
     * @param node
     * @param path
     */
    public ManagedFileSplit(String node, String path) {
        super(node, path);
    }

    @Override
    public String toString() {
        return "Node: " + getNodeName() + " managed path: " + getPath();
    }

    /**
     * Get the local file represented by this split
     *
     * @param ioManager
     * @return
     * @throws HyracksDataException
     */
    @Override
    public File getFile(IIOManager ioManager) throws HyracksDataException {
        return getFileReference(ioManager).getFile();
    }

    /**
     * Get the file reference for the split
     *
     * @param ioManager
     * @return
     * @throws HyracksDataException
     */
    @Override
    public FileReference getFileReference(IIOManager ioManager) throws HyracksDataException {
        return ioManager.resolve(getPath());
    }
}
