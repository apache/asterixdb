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
package org.apache.asterix.lang.common.struct;

public final class VarIdentifier extends Identifier {
    private int id = 0;

    public VarIdentifier() {
        super();
    }

    public VarIdentifier(String value) {
        super();
        this.value = value;
    }

    public VarIdentifier(String value, int id) {
        super();
        this.value = value;
        this.id = id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public VarIdentifier clone() {
        VarIdentifier vi = new VarIdentifier(this.value);
        vi.setId(this.id);
        return vi;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VarIdentifier)) {
            return false;
        }
        VarIdentifier vid = (VarIdentifier) obj;
        return value.equals(vid.value);
    }
}
