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

import java.io.Serial;
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.api.IMetadataEntity;

public class CoordinateReferenceSystem implements IMetadataEntity<CoordinateReferenceSystem> {

    @Serial
    private static final long serialVersionUID = 2L;

    private final String databaseName;
    private final DataverseName dataverseName;
    private final int srid;
    private final String crsName;
    private final String crsWkt;

    public CoordinateReferenceSystem(String databaseName, DataverseName dataverseName, int srid, String crsName,
            String crsWkt) {
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.srid = srid;
        this.crsName = crsName;
        this.crsWkt = crsWkt;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public int getSrid() {
        return srid;
    }

    public String getCrsName() {
        return crsName;
    }

    public String getCrsWkt() {
        return crsWkt;
    }

    @Override
    public CoordinateReferenceSystem addToCache(MetadataCache cache) {
        return cache.addOrUpdateCrs(this);
    }

    @Override
    public CoordinateReferenceSystem dropFromCache(MetadataCache cache) {
        return cache.dropCrs(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CoordinateReferenceSystem that)) {
            return false;
        }
        return srid == that.srid && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(dataverseName, that.dataverseName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, dataverseName, srid);
    }
}
