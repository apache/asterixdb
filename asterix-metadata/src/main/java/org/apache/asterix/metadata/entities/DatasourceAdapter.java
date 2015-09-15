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
import org.apache.asterix.metadata.feeds.AdapterIdentifier;

public class DatasourceAdapter implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

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
