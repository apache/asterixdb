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
package org.apache.asterix.common.api;

import java.io.Serializable;
import java.util.Objects;

public class ExtensionId implements Serializable {

    private static final long serialVersionUID = 1L;
    private final String name;
    private final int version;

    public ExtensionId(String name, int version) {
        this.name = name;
        this.version = version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName().hashCode(), version);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (o instanceof ExtensionId) {
            ExtensionId oExtensionId = (ExtensionId) o;
            return version == oExtensionId.version && Objects.equals(name, oExtensionId.getName());
        }
        return false;
    }

    public int getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return name + ":" + version;
    }
}
