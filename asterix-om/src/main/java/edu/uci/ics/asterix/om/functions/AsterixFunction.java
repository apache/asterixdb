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
package edu.uci.ics.asterix.om.functions;

import java.io.Serializable;

public class AsterixFunction implements Serializable {

    private final String name;
    private final int arity;
    public final static int VARARGS = -1;

    public AsterixFunction(String name, int arity) throws IllegalArgumentException {
        this.name = name;
        this.arity = arity;
    }

    public String getName() {
        return name;
    }

    public int getArity() {
        return arity;
    }

    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AsterixFunction)) {
            return false;
        }
        if (name.equals(((AsterixFunction) o).getName()) || arity == VARARGS) {
            return true;
        }
        return false;
    }

}
