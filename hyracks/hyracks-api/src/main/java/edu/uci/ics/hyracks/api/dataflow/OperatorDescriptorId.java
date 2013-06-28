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

public final class OperatorDescriptorId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int id;

    public OperatorDescriptorId(int id) {
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
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof OperatorDescriptorId)) {
            return false;
        }
        return ((OperatorDescriptorId) o).id == id;
    }

    @Override
    public String toString() {
        return "ODID:" + id;
    }

    public static OperatorDescriptorId parse(String str) {
        if (str.startsWith("ODID:")) {
            str = str.substring(5);
            return new OperatorDescriptorId(Integer.parseInt(str));
        }
        throw new IllegalArgumentException("Unable to parse: " + str);
    }
}