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

package org.apache.asterix.metadata.entities;

import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a dataverse.
 */
public class Dataverse implements IMetadataEntity<Dataverse> {

    private static final long serialVersionUID = 1L;
    // Enforced to be unique within an Asterix cluster..
    private final String dataverseName;
    private final String dataFormat;
    private final int pendingOp;

    public Dataverse(String dataverseName, String format, int pendingOp) {
        this.dataverseName = dataverseName;
        this.dataFormat = format;
        this.pendingOp = pendingOp;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public int getPendingOp() {
        return pendingOp;
    }

    @Override
    public Dataverse addToCache(MetadataCache cache) {
        return cache.addDataverseIfNotExists(this);
    }

    @Override
    public Dataverse dropFromCache(MetadataCache cache) {
        return cache.dropDataverse(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + dataverseName;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Dataverse)) {
            return false;
        }
        Dataverse other = (Dataverse) o;
        return dataverseName.equals(other.getDataverseName());
    }

    @Override
    public int hashCode() {
        return dataverseName.hashCode();
    }

}
