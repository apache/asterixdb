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
    private static final long serialVersionUID = 1L;
    private final long resourceId;
    private final String resourceName;
    private final int partition;
    private final int resourceType;
    private final Object object;

    public static final int TransientResource = 0;
    public static final int LSMBTreeResource = 1;
    public static final int LSMRTreeResource = 2;
    public static final int LSMInvertedIndexResource = 3;
    public static final int ExternalBTreeResource = 4;
    public static final int ExternalRTreeResource = 5;
    public static final int ExternalBTreeWithBuddyResource = 6;

    public LocalResource(long resourceId, String resourceName, int partition, int resourceType, Object object) {
        this.resourceId = resourceId;
        this.resourceName = resourceName;
        this.partition = partition;
        this.resourceType = resourceType;
        this.object = object;
    }

    public long getResourceId() {
        return resourceId;
    }

    public String getResourceName() {
        return resourceName;
    }

    public int getPartition() {
        return partition;
    }

    public int getResourceType() {
        return resourceType;
    }

    public Object getResourceObject() {
        return object;
    }
}
