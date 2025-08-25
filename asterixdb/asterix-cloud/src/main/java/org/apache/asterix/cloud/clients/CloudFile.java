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
package org.apache.asterix.cloud.clients;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class CloudFile {
    private static final long IGNORED_SIZE = -1;
    private final String path;
    private final long size;

    private CloudFile(String path, long size) {
        this.path = path;
        this.size = size;
    }

    public String getPath() {
        return path;
    }

    public long getSize() {
        return size;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CloudFile other)) {
            return false;
        }

        return path.equals(other.path) && compareSize(other.size);
    }

    @Override
    public String toString() {
        return path + '[' + size + ']';
    }

    private boolean compareSize(long otherSize) {
        // Compare sizes if both sizes are not ignored
        return size == otherSize || size == IGNORED_SIZE || otherSize == IGNORED_SIZE;
    }

    public static CloudFile of(String path, long size) {
        return new CloudFile(path, size);
    }

    public static CloudFile of(String path) {
        return new CloudFile(path, IGNORED_SIZE);
    }

    public static Map<String, CloudFile> toMap(Set<CloudFile> cloudFiles) {
        Map<String, CloudFile> map = new HashMap<>();
        for (CloudFile cloudFile : cloudFiles) {
            map.put(cloudFile.getPath(), cloudFile);
        }

        return map;
    }
}
