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
package org.apache.asterix.aql.context;

import java.util.HashMap;

import org.apache.asterix.aql.expression.Identifier;
import org.apache.asterix.aql.expression.VarIdentifier;
import org.apache.asterix.aql.parser.ScopeChecker;
import org.apache.asterix.common.functions.FunctionSignature;

public final class Scope {
    private Scope parent;
    private HashMap<String, Identifier> symbols = null;
    private FunctionSignatures functionSignatures = null;
    private final ScopeChecker scopeChecker;
    private boolean maskParentScope = false;

    public Scope(ScopeChecker scopeChecker, Scope parent) {
        this.scopeChecker = scopeChecker;
        this.parent = parent;
    }

    public Scope(ScopeChecker scopeChecker) {
        this(scopeChecker, null);
    }

    public Scope(ScopeChecker scopeChecker, Scope parent, boolean maskParentScope) {
        this(scopeChecker, parent);
        this.maskParentScope = maskParentScope;
    }

    /**
     * Find a symbol in the scope
     * 
     * @param name
     * @return the Identifier of this symbol; otherwise null;
     */
    public Identifier findSymbol(String name) {
        Identifier ident = null;
        if (symbols != null) {
            ident = symbols.get(name);
        }
        if (ident == null && !maskParentScope && parent != null) {
            ident = parent.findSymbol(name);
        }
        return ident;
    }

    public Identifier findLocalSymbol(String name) {
        if (symbols != null) {
            return symbols.get(name);
        }
        return null;
    }

    /**
     * Add a symbol into scope
     * 
     * @param ident
     */
    public void addSymbolToScope(Identifier ident) {
        if (symbols == null) {
            symbols = new HashMap<String, Identifier>();
        }
        symbols.put(ident.getValue(), ident);
    }

    public void addNewVarSymbolToScope(VarIdentifier ident) {
        scopeChecker.incVarCounter();
        ident.setId(scopeChecker.getVarCounter());
        addSymbolToScope(ident);
    }

    /**
     * Add a FunctionDescriptor into functionSignatures
     * 
     * @param fd
     *            FunctionDescriptor
     * @param varargs
     *            whether this function has varargs
     */
    public void addFunctionDescriptor(FunctionSignature signature, boolean varargs) {
        if (functionSignatures == null) {
            functionSignatures = new FunctionSignatures();
        }
        functionSignatures.put(signature, varargs);
    }

    /**
     * find a function signature
     * 
     * @param name
     *            name of the function
     * @param arity
     *            # of arguments
     * @return FunctionDescriptor of the function found; otherwise null
     */
    public FunctionSignature findFunctionSignature(String dataverse, String name, int arity) {
        FunctionSignature fd = null;
        if (functionSignatures != null) {
            fd = functionSignatures.get(dataverse, name, arity);
        }
        if (fd == null && parent != null) {
            fd = parent.findFunctionSignature(dataverse, name, arity);
        }
        return fd;
    }

}
