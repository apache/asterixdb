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
package org.apache.asterix.om.types;

import java.io.Serializable;
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;

public class TypeSignature implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DataverseName dataverseName;
    private final String name;

    public TypeSignature(DataverseName dataverseName, String name) {
        this.dataverseName = dataverseName;
        this.name = name;
    }

    public TypeSignature(BuiltinType builtinType) {
        this(null, builtinType.getTypeName());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TypeSignature)) {
            return false;
        } else {
            TypeSignature f = ((TypeSignature) o);
            return Objects.equals(dataverseName, f.getDataverseName()) && name.equals(f.getName());
        }
    }

    @Override
    public String toString() {
        return (dataverseName != null ? dataverseName.getCanonicalForm() : null) + "@" + name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, name);
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getName() {
        return name;
    }
}
