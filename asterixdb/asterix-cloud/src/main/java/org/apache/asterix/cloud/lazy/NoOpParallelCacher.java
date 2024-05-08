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
package org.apache.asterix.cloud.lazy;

import java.io.FilenameFilter;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

public class NoOpParallelCacher implements IParallelCacher {
    public static final IParallelCacher INSTANCE = new NoOpParallelCacher();

    @Override
    public boolean isCacheable(FileReference fileReference) {
        return false;
    }

    @Override
    public Set<FileReference> getUncachedFiles(FileReference dir, FilenameFilter filter) {
        return Collections.emptySet();
    }

    @Override
    public long getSize(FileReference fileReference) {
        return 0L;
    }

    @Override
    public long getUncachedTotalSize() {
        return 0L;
    }

    @Override
    public boolean createEmptyDataFiles(FileReference indexFile) throws HyracksDataException {
        return false;
    }

    @Override
    public boolean downloadDataFiles(FileReference indexFile) {
        return false;
    }

    @Override
    public boolean downloadMetadataFiles(FileReference indexFile) {
        return false;
    }

    @Override
    public boolean remove(Collection<FileReference> deletedFiles) {
        return false;
    }

    @Override
    public boolean remove(FileReference fileReference) {
        return false;
    }

    @Override
    public void add(Collection<FileReference> files) {
        // NoOp
    }

    @Override
    public void close() {
        // NoOp
    }
}
