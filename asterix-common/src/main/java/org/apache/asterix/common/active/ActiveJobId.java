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

import org.apache.asterix.common.active.ActiveObjectId.ActiveObjectType;

/**
 * A unique identifier for a an active job. There
 * is not always a one-to-one correspondence between active
 * objects and active jobs. In the case of feeds, one feed
 * can have many active jobs, represented by feedconnectionid
 */
public class ActiveJobId implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final ActiveObjectId activeId;

    public ActiveJobId(ActiveObjectId activeId) {
        this.activeId = activeId;
    }

    public ActiveJobId(String dataverse, String name, ActiveObjectType type) {
        this.activeId = new ActiveObjectId(dataverse, name, type);
    }

    public ActiveObjectId getActiveId() {
        return activeId;
    }

    public String getDataverse() {
        return activeId.getDataverse();
    }

    public String getName() {
        return activeId.getName();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ActiveJobId)) {
            return false;
        }

        if (this == o || ((ActiveJobId) o).getActiveId().equals(activeId)) {
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
        return "Job for :" + activeId.toString();
    }

}