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

import edu.uci.ics.asterix.external.dataset.adapter.AdapterIdentifier;
import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

public class DatasourceAdapter implements IMetadataEntity {

    public enum AdapterType {
        INTERNAL,
        EXTERNAL
    }

    private final AdapterIdentifier adapterIdentifier;
    private final String classname;
    private final AdapterType type;

    public DatasourceAdapter(AdapterIdentifier adapterIdentifier, String classname, AdapterType type) {
        this.adapterIdentifier = adapterIdentifier;
        this.classname = classname;
        this.type = type;
    }

    @Override
    public Object addToCache(MetadataCache cache) {
        return cache.addAdapterIfNotExists(this);
    }

    @Override
    public Object dropFromCache(MetadataCache cache) {
        return cache.dropAdapter(this);
    }

    public AdapterIdentifier getAdapterIdentifier() {
        return adapterIdentifier;
    }

    public String getClassname() {
        return classname;
    }

    public AdapterType getType() {
        return type;
    }

}
