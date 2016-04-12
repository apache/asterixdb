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
import org.apache.asterix.om.types.IAType;

/**
 * Metadata describing a datatype.
 */
public class Datatype implements IMetadataEntity<Datatype> {

    private static final long serialVersionUID = 1L;

    private final String dataverseName;
    // Enforced to be unique within a dataverse.
    private final String datatypeName;
    private final IAType datatype;
    private final boolean isAnonymous;

    public Datatype(String dataverseName, String datatypeName, IAType datatype, boolean isAnonymous) {
        this.dataverseName = dataverseName;
        this.datatypeName = datatypeName;
        this.datatype = datatype;
        this.isAnonymous = isAnonymous;
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public String getDatatypeName() {
        return datatypeName;
    }

    public IAType getDatatype() {
        return datatype;
    }

    public boolean getIsAnonymous() {
        return isAnonymous;
    }

    @Override
    public Datatype addToCache(MetadataCache cache) {
        return cache.addDatatypeIfNotExists(this);
    }

    @Override
    public Datatype dropFromCache(MetadataCache cache) {
        return cache.dropDatatype(this);
    }
}
