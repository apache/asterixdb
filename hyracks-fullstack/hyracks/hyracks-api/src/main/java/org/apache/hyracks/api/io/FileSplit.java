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

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A node and a path (Can be managed: inside the IO device or absolute inside or outside IO devices)
 * Used to identify a file/dir across the cluster.
 */
public abstract class FileSplit implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String node;
    private final String path;

    /**
     * Constructor
     *
     * @param node
     * @param path
     */
    protected FileSplit(String node, String path) {
        this.node = node;
        this.path = path;
    }

    /**
     * @return the path
     */
    public String getPath() {
        return path;
    }

    /**
     * Get the local file represented by this split
     *
     * @param ioManager
     * @return
     * @throws HyracksDataException
     */
    public abstract File getFile(IIOManager ioManager) throws HyracksDataException;

    /**
     * @return the node
     */
    public String getNodeName() {
        return node;
    }

    // TODO(amoudi): This method should be removed from this class and moved to ManagedFileSplit since it is only
    // applicable for that subclass
    public abstract FileReference getFileReference(IIOManager ioManager) throws HyracksDataException;
}
