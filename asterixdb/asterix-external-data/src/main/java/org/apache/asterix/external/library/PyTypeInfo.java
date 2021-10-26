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
package org.apache.asterix.external.library;

import java.io.DataOutput;
import java.util.Objects;

import org.apache.asterix.om.types.IAType;

public class PyTypeInfo {

    private final IAType type;
    private final DataOutput out;

    public PyTypeInfo(IAType type, DataOutput out) {
        this.type = type;
        this.out = out;
    }

    public DataOutput getDataOutput() {
        return out;
    }

    public IAType getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(out, type);
    }

    @Override
    public boolean equals(Object obj) {
        return out.equals(out) && type.equals(type);
    }

}
