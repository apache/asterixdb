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
import org.apache.asterix.runtime.fulltext.FullTextConfigDescriptor;

public class FullTextConfigMetadataEntity implements IMetadataEntity<FullTextConfigMetadataEntity> {
    private static final long serialVersionUID = -8257829613982301855L;

    private final FullTextConfigDescriptor fullTextConfig;

    public FullTextConfigMetadataEntity(FullTextConfigDescriptor config) {
        this.fullTextConfig = config;
    }

    public FullTextConfigDescriptor getFullTextConfig() {
        return fullTextConfig;
    }

    @Override
    public FullTextConfigMetadataEntity addToCache(MetadataCache cache) {
        return cache.addFullTextConfigIfNotExists(this);
    }

    @Override
    public FullTextConfigMetadataEntity dropFromCache(MetadataCache cache) {
        return cache.dropFullTextConfig(this);
    }

    public static FullTextConfigMetadataEntity getDefaultFullTextConfigMetadataEntity() {
        return new FullTextConfigMetadataEntity(FullTextConfigDescriptor.getDefaultFullTextConfig());
    }

}
