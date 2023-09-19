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

/**
 * Metadata describing a database.
 */
public class Database implements IMetadataEntity<Database> {

    private static final long serialVersionUID = 1L;

    private final String databaseName;
    private final boolean isSystemDatabase;
    private final int pendingOp;

    public Database(String databaseName, boolean isSystemDatabase, int pendingOp) {
        this.databaseName = databaseName;
        this.isSystemDatabase = isSystemDatabase;
        this.pendingOp = pendingOp;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public boolean isSystemDatabase() {
        return isSystemDatabase;
    }

    public int getPendingOp() {
        return pendingOp;
    }

    @Override
    public Database addToCache(MetadataCache cache) {
        return cache.addDatabaseIfNotExists(this);
    }

    @Override
    public Database dropFromCache(MetadataCache cache) {
        return cache.dropDatabase(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + databaseName;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Database)) {
            return false;
        }
        Database other = (Database) o;
        return Objects.equals(databaseName, other.databaseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName);
    }
}
