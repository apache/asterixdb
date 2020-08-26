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
package org.apache.hyracks.algebricks.core.algebra.functions;

import java.io.Serializable;
import java.util.Objects;

public class FunctionIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String name;
    private final int arity;

    public static final int VARARGS = -1;

    public FunctionIdentifier(String namespace, String name, int arity) {
        this.namespace = namespace;
        this.name = name;
        this.arity = arity;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getName() {
        return name;
    }

    public int getArity() {
        return arity;
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            return true;
        }
        if (o instanceof FunctionIdentifier) {
            FunctionIdentifier ofi = (FunctionIdentifier) o;
            return namespace.equals(ofi.namespace) && name.equals(ofi.name) && arity == ofi.arity;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, name, arity);
    }

    @Override
    public String toString() {
        return namespace + ':' + name + '#' + arity;
    }
}
