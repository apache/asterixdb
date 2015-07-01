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
package edu.uci.ics.asterix.metadata.feeds;

import java.io.Serializable;

/**
 * A unique identifier for a data source adapter.
 */
public class AdapterIdentifier implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String name;

    public AdapterIdentifier(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        return (namespace + "@" + name).hashCode();

    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (!(o instanceof AdapterIdentifier)) {
            return false;
        }
        return namespace.equals(((AdapterIdentifier) o).getNamespace())
                && name.equals(((AdapterIdentifier) o).getName());
    }
}