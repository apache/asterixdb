/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.entities;

import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;
import edu.uci.ics.asterix.om.types.IAType;

/**
 * Metadata describing a datatype.
 */
public class Datatype implements IMetadataEntity {

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
    public Object addToCache(MetadataCache cache) {
        return cache.addDatatypeIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropDatatype(this);
    }
}
