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
package org.apache.asterix.common.active;

import java.io.Serializable;

/**
 * A unique identifier for a an active object. There
 * is not always a one-to-one correspondence between active
 * objects and active jobs. In the case of feeds, one feed
 * can have many active jobs.
 */
public class ActiveId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String dataverse;
    private final String name;

    public enum ActiveObjectType {
        FEED,
        CHANNEL,
        PROCEDURE
    }

    private final ActiveObjectType type;

    public ActiveId(String dataverse, String name, ActiveObjectType type) {
        this.dataverse = dataverse;
        this.name = name;
        this.type = type;
    }

    public String getDataverse() {
        return dataverse;
    }

    public String getName() {
        return name;
    }

    public ActiveObjectType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ActiveId)) {
            return false;
        }
        if (this == o || ((ActiveId) o).getName().equals(name) && ((ActiveId) o).getDataverse().equals(dataverse)
                && ((ActiveId) o).getType().equals(type)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        return dataverse + "." + name + "(" + type + ")";
    }
}
