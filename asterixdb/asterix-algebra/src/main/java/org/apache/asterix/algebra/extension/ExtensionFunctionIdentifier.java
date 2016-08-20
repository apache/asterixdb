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
package org.apache.asterix.algebra.extension;

import org.apache.asterix.common.api.ExtensionId;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * an Identifier for an extension function
 */
public class ExtensionFunctionIdentifier extends FunctionIdentifier {
    //TODO (till): remove this classs

    private static final long serialVersionUID = 1L;
    private final ExtensionId extensionId;

    /**
     * Create an identifier for an external function
     *
     * @param namespace
     * @param name
     * @param extensionId
     */
    public ExtensionFunctionIdentifier(String namespace, String name, ExtensionId extensionId) {
        super(namespace, name);
        this.extensionId = extensionId;
    }

    /**
     * Create an identifier for an external function
     *
     * @param namespace
     * @param name
     * @param arity
     * @param extensionId
     */
    public ExtensionFunctionIdentifier(String namespace, String name, int arity, ExtensionId extensionId) {
        super(namespace, name, arity);
        this.extensionId = extensionId;
    }

    public ExtensionId getExtensionId() {
        return extensionId;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + extensionId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ExtensionFunctionIdentifier) {
            ExtensionFunctionIdentifier oId = (ExtensionFunctionIdentifier) o;
            return super.equals(o) && extensionId.equals(oId.getExtensionId());
        }
        return false;
    }
}
