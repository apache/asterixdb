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
import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FunctionSignature implements Serializable {

    private static final long serialVersionUID = 2L;

    private DataverseName dataverseName;

    private String name;

    private int arity;

    public FunctionSignature(FunctionIdentifier fi) {
        this(getDataverseName(fi), fi.getName(), fi.getArity());
    }

    public FunctionSignature(DataverseName dataverseName, String name, int arity) {
        this.dataverseName = dataverseName;
        this.name = name;
        this.arity = arity;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FunctionSignature)) {
            return false;
        }
        FunctionSignature f = ((FunctionSignature) o);
        return Objects.equals(dataverseName, f.dataverseName) && name.equals(f.name)
                && (arity == f.arity || arity == FunctionIdentifier.VARARGS || f.arity == FunctionIdentifier.VARARGS);
    }

    @Override
    public String toString() {
        return toString(true);
    }

    public String toString(boolean includeArity) {
        String namespaceCanonicalForm = dataverseName != null ? dataverseName.getCanonicalForm() : null;
        int len = (namespaceCanonicalForm != null ? namespaceCanonicalForm.length() : 4) + 1 + name.length()
                + (includeArity ? 3 : 0);
        StringBuilder sb = new StringBuilder(len);
        sb.append(namespaceCanonicalForm).append('.').append(name);
        if (includeArity) {
            sb.append('@').append(arity);
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, name);
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getName() {
        return name;
    }

    public int getArity() {
        return arity;
    }

    public void setDataverseName(DataverseName dataverseName) {
        this.dataverseName = dataverseName;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setArity(int arity) {
        this.arity = arity;
    }

    public FunctionIdentifier createFunctionIdentifier() {
        return createFunctionIdentifier(dataverseName, name, arity);
    }

    public static FunctionIdentifier createFunctionIdentifier(DataverseName dataverseName, String functionName,
            int arity) {
        return new FunctionIdentifier(dataverseName.getCanonicalForm(), functionName, arity);
    }

    public static FunctionIdentifier createFunctionIdentifier(DataverseName dataverseName, String functionName) {
        return new FunctionIdentifier(dataverseName.getCanonicalForm(), functionName);
    }

    public static DataverseName getDataverseName(FunctionIdentifier fi) {
        String dataverseCanonicalName = fi.getNamespace();
        switch (dataverseCanonicalName) {
            case FunctionConstants.ASTERIX_NS:
                return FunctionConstants.ASTERIX_DV;
            case AlgebricksBuiltinFunctions.ALGEBRICKS_NS:
                return FunctionConstants.ALGEBRICKS_DV;
            default:
                return DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        }
    }
}
