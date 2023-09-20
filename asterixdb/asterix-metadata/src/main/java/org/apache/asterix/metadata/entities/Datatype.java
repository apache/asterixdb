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

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.MetadataCache;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.api.IMetadataEntity;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

/**
 * Metadata describing a datatype.
 */
public class Datatype implements IMetadataEntity<Datatype> {

    private static final long serialVersionUID = 3L;

    private final String databaseName;
    private final DataverseName dataverseName;
    private final String datatypeName;
    private final IAType datatype;
    private final boolean isAnonymous;

    public Datatype(String databaseName, DataverseName dataverseName, String datatypeName, IAType datatype,
            boolean isAnonymous) {
        this.databaseName = Objects.requireNonNull(databaseName);
        this.dataverseName = dataverseName;
        this.datatypeName = datatypeName;
        this.datatype = datatype;
        this.isAnonymous = isAnonymous;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
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

    public static IAType getTypeFromTypeName(MetadataNode metadataNode, TxnId txnId, String database,
            DataverseName dataverseName, String typeName) throws AlgebricksException {
        IAType type = BuiltinTypeMap.getBuiltinType(typeName);
        if (type == null) {
            Datatype dt = metadataNode.getDatatype(txnId, database, dataverseName, typeName);
            if (dt != null) {
                type = dt.getDatatype();
            }
        }
        return type;
    }
}
