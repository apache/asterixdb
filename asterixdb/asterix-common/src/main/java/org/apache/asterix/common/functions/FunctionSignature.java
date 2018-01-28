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
package org.apache.asterix.common.functions;

import java.io.Serializable;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FunctionSignature implements Serializable {
    private static final long serialVersionUID = 1L;
    private String namespace;
    private String name;
    private int arity;

    public FunctionSignature(FunctionIdentifier fi) {
        this(fi.getNamespace(), fi.getName(), fi.getArity());
    }

    public FunctionSignature(String namespace, String name, int arity) {
        this.namespace = namespace;
        this.name = name;
        this.arity = arity;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FunctionSignature)) {
            return false;
        } else {
            FunctionSignature f = ((FunctionSignature) o);
            return ((namespace != null && namespace.equals(f.getNamespace())
                    || (namespace == null && f.getNamespace() == null))) && name.equals(f.getName())
                    && (arity == f.getArity() || arity == FunctionIdentifier.VARARGS
                            || f.getArity() == FunctionIdentifier.VARARGS);
        }
    }

    @Override
    public String toString() {
        return namespace + "." + name + "@" + arity;
    }

    @Override
    public int hashCode() {
        return (namespace + "." + name).hashCode();
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

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setArity(int arity) {
        this.arity = arity;
    }

}
