/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.api.topology;

import java.io.Serializable;
import java.util.Map;

public abstract class NetworkEndpoint implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum EndpointType {
        NETWORK_SWITCH,
        NETWORK_TERMINAL,
    }

    protected final String name;

    protected final Map<String, String> properties;

    public NetworkEndpoint(String name, Map<String, String> properties) {
        this.name = name;
        this.properties = properties;
    }

    public abstract EndpointType getType();

    public final String getName() {
        return name;
    }

    public final Map<String, String> getProperties() {
        return properties;
    }
}