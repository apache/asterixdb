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

import org.apache.asterix.common.metadata.DataverseName;

/**
 * This class provides static factory methods for creating entity details.
 */

public class EntityDetails {

    public enum EntityType {
        DATASET,
        VIEW,
        FUNCTION,
        DATABASE,
        DATAVERSE,
        SYNONYM,
        INDEX
    }

    private final String databaseName;
    private final DataverseName dataverseName;
    private final String entityName;
    private final EntityType entityType;
    // The number of arguments that the function accepts. Relevant only for function entity.
    private int functionArity;

    private EntityDetails(String databaseName, DataverseName dataverseName, String entityName, EntityType entityType) {
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.entityName = entityName;
        this.entityType = entityType;
    }

    private EntityDetails(String databaseName, DataverseName dataverseName, String entityName, EntityType entityType,
            int functionArity) {
        this.databaseName = databaseName;
        this.dataverseName = dataverseName;
        this.entityName = entityName;
        this.entityType = entityType;
        this.functionArity = functionArity;
    }

    public static EntityDetails newDatabase(String databaseName) {
        return new EntityDetails(databaseName, null, null, EntityType.DATABASE);
    }

    public static EntityDetails newDataverse(String databaseName, DataverseName dataverseName) {
        return new EntityDetails(databaseName, dataverseName, null, EntityType.DATAVERSE);
    }

    public static EntityDetails newDataset(String databaseName, DataverseName dataverseName, String datasetName) {
        return new EntityDetails(databaseName, dataverseName, datasetName, EntityType.DATASET);
    }

    public static EntityDetails newView(String databaseName, DataverseName dataverseName, String viewName) {
        return new EntityDetails(databaseName, dataverseName, viewName, EntityType.VIEW);
    }

    public static EntityDetails newFunction(String databaseName, DataverseName dataverseName, String functionName,
            int functionArity) {
        return new EntityDetails(databaseName, dataverseName, functionName, EntityType.FUNCTION, functionArity);
    }

    public static EntityDetails newSynonym(String databaseName, DataverseName dataverseName, String synonymName) {
        return new EntityDetails(databaseName, dataverseName, synonymName, EntityType.SYNONYM);
    }

    public static EntityDetails newIndex(String databaseName, DataverseName dataverseName, String indexName) {
        return new EntityDetails(databaseName, dataverseName, indexName, EntityType.INDEX);
    }

    public static EntityDetails newExtension(String extensionName) {
        return new EntityDetails(null, null, extensionName, null);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getEntityName() {
        return entityName;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public int getFunctionArity() {
        return functionArity;
    }

    public static String getFunctionNameWithArity(String functionName, int functionArity) {
        return functionName + "(" + functionArity + ")";
    }
}
