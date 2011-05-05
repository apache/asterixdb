/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.api.things;

import java.io.Serializable;

public final class ThingDescriptorId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long id;

    public ThingDescriptorId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "TID: " + id;
    }

    public int hashCode() {
        return (int) (id & 0xffffffff);
    }

    public boolean equals(Object o) {
        if (!(o instanceof ThingDescriptorId)) {
            return false;
        }
        return id == ((ThingDescriptorId) o).id;
    }
}