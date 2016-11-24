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
package org.apache.hyracks.storage.common.file;

import java.io.Serializable;

public class LocalResource implements Serializable {
    private static final long serialVersionUID = 2L;
    /*
     * TODO: Types should move somewhere else
     */
    public static final int TransientResource = 0;
    public static final int LSMBTreeResource = 1;
    public static final int LSMRTreeResource = 2;
    public static final int LSMInvertedIndexResource = 3;
    public static final int ExternalBTreeResource = 4;
    public static final int ExternalRTreeResource = 5;
    public static final int ExternalBTreeWithBuddyResource = 6;
    /*
     * object members
     */
    /**
     * Globally unique id of the local resource
     */
    private final long id;
    /**
     * Relative path of the resource
     */
    private final String path;
    /**
     * An Integer identifying the resource type
     */
    private final int type;
    /**
     * Storage version
     */
    private final int version;
    /**
     * The serializable application dependent on the application
     */
    private final Serializable resource;

    /**
     * Construct an NC local resource
     * @param id
     *            Globally unique id of the local resource
     * @param path
     *            Relative path of the resource
     * @param type
     *            An Integer identifying the resource type
     * @param version
     *            Storage version
     * @param resource
     *            The serializable application dependent on the application
     */
    public LocalResource(long id, String path, int type,
            int version, Serializable resource) {
        this.id = id;
        this.path = path;
        this.type = type;
        this.resource = resource;
        this.version = version;
    }

    /**
     * @return the resource id
     */
    public long getId() {
        return id;
    }

    /**
     * @return the resource relative path
     */
    public String getPath() {
        return path;
    }

    /**
     * @return the resource type
     */
    public int getType() {
        return type;
    }

    /**
     * @return the serializable resource
     */
    public Serializable getResource() {
        return resource;
    }

    /**
     * @return the storage version
     */
    public int getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return new StringBuilder("{\"").append(LocalResource.class.getSimpleName()).append("\" : ").append("{\"id\" = ")
                .append(id).append(", \"name\" : \"").append(path).append("\", \"type\" : ").append(type).append(
                        ", \"resource\" : ").append(resource).append(", \"version\" : ").append(version).append(" } ")
                .toString();
    }
}
