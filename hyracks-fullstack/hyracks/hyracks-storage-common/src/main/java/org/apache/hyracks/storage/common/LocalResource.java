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
package org.apache.hyracks.storage.common;

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LocalResource implements Serializable, IJsonSerializable {
    private static final long serialVersionUID = 2L;
    /*
     * object members
     */
    /**
     * Globally unique id of the local resource
     */
    private final long id;
    /**
     * Storage version
     */
    private final int version;
    /**
     * Should be persisted?
     */
    private final boolean durable;
    /**
     * The serializable application dependent on the application
     */
    private final IResource resource;

    /**
     * Construct an NC local resource
     *
     * @param id
     *            Globally unique id of the local resource
     * @param path
     *            Relative path of the resource
     * @param version
     *            Storage version
     * @param resource
     *            The serializable application dependent on the application
     */
    public LocalResource(long id, int version, boolean durable, IResource resource) {
        this.id = id;
        this.version = version;
        this.durable = durable;
        this.resource = resource;
    }

    /**
     * @return the resource id
     */
    public long getId() {
        return id;
    }

    /**
     * @return the resource relative path
     */
    public String getPath() {
        return resource.getPath();
    }

    /**
     * @return the serializable resource
     */
    public IResource getResource() {
        return resource;
    }

    /**
     * @return the storage version
     */
    public int getVersion() {
        return version;
    }

    /**
     * @return true if should be persisted, false otherwise
     */
    public boolean isDurable() {
        return durable;
    }

    @Override
    public String toString() {
        return new StringBuilder("{\"").append(LocalResource.class.getSimpleName()).append("\" : ").append("{\"id\" = ")
                .append(id).append(", \"resource\" : ").append(resource).append(", \"version\" : ").append(version)
                .append(" } ").toString();
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("id", id);
        json.put("version", version);
        json.put("durable", durable);
        json.set("resource", resource.toJson(registry));
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final long id = json.get("id").asLong();
        final int version = json.get("version").asInt();
        final boolean durable = json.get("durable").asBoolean();
        final IResource resource = (IResource) registry.deserialize(json.get("resource"));
        return new LocalResource(id, version, durable, resource);
    }
}
