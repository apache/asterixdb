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

package org.apache.asterix.runtime.evaluators.staticcodegen;

/**
 * The data structure that can uniquely identify a method.
 */
public class MethodIdentifier {

    private final String name;
    private final String desc;
    private final String signature;

    public MethodIdentifier(String name, String desc, String signature) {
        this.name = name == null ? "" : name;
        this.desc = desc == null ? "" : desc;
        this.signature = signature == null ? "" : signature;
    }

    @Override
    public int hashCode() {
        return name.hashCode() * desc.hashCode() * signature.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MethodIdentifier)) {
            return false;
        }
        MethodIdentifier methodIdentifier = (MethodIdentifier) o;
        return name.equals(methodIdentifier.name) && desc.equals(methodIdentifier.desc)
                && signature.equals(methodIdentifier.signature);
    }

    @Override
    public String toString() {
        return name + ":" + desc + ":" + signature;
    }

}
