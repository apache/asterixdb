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
package edu.uci.ics.asterix.om.types;

import java.io.Serializable;

public class TypeSignature implements Serializable {

    private final String dataverse;
    private final String name;
    private final String alias;

    public TypeSignature(String namespace, String name) {
        this.dataverse = namespace;
        this.name = name;
        this.alias = dataverse + "@" + name;
    }

    public boolean equals(Object o) {
        if (!(o instanceof TypeSignature)) {
            return false;
        } else {
            TypeSignature f = ((TypeSignature) o);
            return dataverse.equals(f.getNamespace()) && name.equals(f.getName());
        }
    }

    public String toString() {
        return alias;
    }

    public int hashCode() {
        return alias.hashCode();
    }

    public String getNamespace() {
        return dataverse;
    }

    public String getName() {
        return name;
    }

}
