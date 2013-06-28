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
package edu.uci.ics.hyracks.api.dataflow;

import java.io.Serializable;

public final class ConnectorDescriptorId implements Serializable {
    private static final long serialVersionUID = 1L;

    private int id;

    public ConnectorDescriptorId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ConnectorDescriptorId)) {
            return false;
        }
        ConnectorDescriptorId other = (ConnectorDescriptorId) obj;
        return id == other.id;
    }

    @Override
    public String toString() {
        return "CDID:" + id;
    }
}