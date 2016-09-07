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
package org.apache.asterix.active;

import java.io.Serializable;

/**
 * A unique identifier for a data feed.
 */
public class EntityId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String extensionName;
    private final String dataverse;
    private final String entityName;
    private final int hashCode;

    public EntityId(String extentionName, String dataverse, String entityName) {
        this.extensionName = extentionName;
        this.dataverse = dataverse;
        this.entityName = entityName;
        this.hashCode = toString().hashCode();
    }

    public String getDataverse() {
        return dataverse;
    }

    public String getEntityName() {
        return entityName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof EntityId)) {
            return false;
        }
        if (this == o || ((EntityId) o).getExtensionName().equals(extensionName)
                && ((EntityId) o).getEntityName().equals(entityName)
                && ((EntityId) o).getDataverse().equals(dataverse)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return dataverse + "." + entityName + "(" + extensionName + ")";
    }

    public String getExtensionName() {
        return extensionName;
    }
}
