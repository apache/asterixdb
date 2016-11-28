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
 * A filesplit for files that are not managed by the IO device
 */
public class UnmanagedFileSplit extends FileSplit {

    private static final long serialVersionUID = 1L;

    public UnmanagedFileSplit(String node, String path) {
        super(node, path);
    }

    @Override
    public String toString() {
        return "Node: " + getNodeName() + " unmanaged path: " + getPath();
    }

    /**
     * Get the local file represented by this split
     *
     * @param ioManager
     * @return
     * @throws HyracksDataException
     */
    @Override
    public File getFile(IIOManager ioManager) {
        return new File(getPath());
    }

    /**
     * Get the local file represented by this unmanaged file split
     *
     * @return
     */
    public File getFile() {
        return new File(getPath());
    }

    @Override
    public FileReference getFileReference(IIOManager ioManager) throws HyracksDataException {
        throw new HyracksDataException("FileReference is only for files inside an IO device");
    }
}
