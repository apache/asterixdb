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
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.Serializable;

/**
 * A unique identifier for a datasource adapter.
 */
public class AdapterIdentifier implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String adapterName;

    public AdapterIdentifier(String namespace, String adapterName) {
        this.namespace = namespace;
        this.adapterName = adapterName;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getAdapterName() {
        return adapterName;
    }

    @Override
    public int hashCode() {
        return (namespace + "@" + adapterName).hashCode();

    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AdapterIdentifier)) {
            return false;
        }
        return namespace.equals(((AdapterIdentifier) o).getNamespace())
                && namespace.equals(((AdapterIdentifier) o).getNamespace());
    }
}