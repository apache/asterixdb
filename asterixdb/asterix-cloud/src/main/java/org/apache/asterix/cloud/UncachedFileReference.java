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
package org.apache.asterix.cloud;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;

public class UncachedFileReference extends FileReference {
    private static final long serialVersionUID = -920468777697935759L;
    private final long size;

    public UncachedFileReference(IODeviceHandle dev, String path, long size) {
        super(dev, path);
        this.size = size;
    }

    private UncachedFileReference(FileReference fileReference) {
        this(fileReference.getDeviceHandle(), fileReference.getRelativePath(), fileReference.getFile().length());
    }

    public long getSize() {
        return size;
    }

    /**
     * Convert a collection of {@link FileReference} to a collection of {@link UncachedFileReference}
     * NOTE: Requires files to exist in the local drive
     *
     * @param files Collection of {@link FileReference} to convert to {@link UncachedFileReference}
     * @return converted collection of file references
     */
    public static Collection<FileReference> toUncached(Collection<FileReference> files) {
        List<FileReference> uncached = new ArrayList<>();
        for (FileReference fileReference : files) {
            if (!fileReference.getFile().exists()) {
                throw new IllegalStateException(fileReference.getAbsolutePath() + " does not exist");
            }
            uncached.add(new UncachedFileReference(fileReference));
        }

        return uncached;
    }
}
