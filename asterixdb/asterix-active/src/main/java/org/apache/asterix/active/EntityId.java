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
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;

/**
 * A unique identifier for a data feed.
 */
public class EntityId implements Serializable {

    private static final long serialVersionUID = 2L;

    private final String extensionName;
    private final DataverseName dataverseName;
    private final String entityName;

    public EntityId(String extentionName, DataverseName dataverseName, String entityName) {
        this.extensionName = extentionName;
        this.dataverseName = dataverseName;
        this.entityName = entityName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getEntityName() {
        return entityName;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof EntityId)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        EntityId other = (EntityId) o;
        return Objects.equals(other.dataverseName, dataverseName) && Objects.equals(other.entityName, entityName)
                && Objects.equals(other.extensionName, extensionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, entityName, extensionName);
    }

    @Override
    public String toString() {
        return dataverseName + "." + entityName + "(" + extensionName + ")";
    }

    public String getExtensionName() {
        return extensionName;
    }
}
