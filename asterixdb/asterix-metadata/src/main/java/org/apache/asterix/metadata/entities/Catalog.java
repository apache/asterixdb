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

import java.util.Objects;

import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.metadata.utils.Creator;

public class Catalog implements IMetadataEntity<Catalog> {

    private static final long serialVersionUID = 2L;

    private final String catalogName;
    private final String catalogType;
    private final int pendingOp;
    private final Creator creator;

    public Catalog(String catalogName, String catalogType, int pendingOp, Creator creator) {
        this.catalogName = catalogName;
        this.catalogType = catalogType;
        this.pendingOp = pendingOp;
        this.creator = creator;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getCatalogType() {
        return catalogType;
    }

    public int getPendingOp() {
        return pendingOp;
    }

    public Creator getCreator() {
        return creator;
    }

    @Override
    public Catalog addToCache(MetadataCache cache) {
        return cache.addOrUpdateCatalog(this);
    }

    @Override
    public Catalog dropFromCache(MetadataCache cache) {
        return cache.dropCatalog(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName);
    }
}
