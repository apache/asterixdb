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
package edu.uci.ics.hyracks.algebricks.core.algebra.functions;

import java.io.Serializable;

public final class FunctionIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String name;
    private final int arity;

    public final static int VARARGS = -1;

    public FunctionIdentifier(String namespace, String name) {
        this(namespace, name, VARARGS);
    }

    public FunctionIdentifier(String namespace, String name, int arity) {
        this.namespace = namespace;
        this.name = name;
        this.arity = arity;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            return true;
        }
        if (o instanceof FunctionIdentifier) {
            FunctionIdentifier ofi = (FunctionIdentifier) o;
            return ofi.getNamespace().equals(getNamespace()) && ofi.name.equals(name);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return name.hashCode() + namespace.hashCode();
    }

    public String toString() {
        return getNamespace() + ":" + name;
    }

    public int getArity() {
        return arity;
    }

    public String getNamespace() {
        return namespace;
    }
}