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
package org.apache.asterix.lang.common.context;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.rewrites.VariableSubstitutionEnvironment;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;

public final class Scope {
    private Scope parent;
    private Map<String, Identifier> symbols = new HashMap<String, Identifier>();
    private Map<String, Expression> varExprMap = new HashMap<String, Expression>();
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
        Identifier ident = symbols.get(name);
        if (ident == null && !maskParentScope && parent != null) {
            ident = parent.findSymbol(name);
        }
        return ident;
    }

    public Identifier findLocalSymbol(String name) {
        return symbols.get(name);
    }

    /**
     * Add a symbol into scope
     *
     * @param ident
     */
    public void addSymbolToScope(Identifier ident) {
        symbols.put(ident.getValue(), ident);
    }

    /**
     * Add a symbol and its definition expression into the scope
     *
     * @param ident
     */
    public void addSymbolExpressionMappingToScope(VariableExpr ident, Expression expr) {
        varExprMap.put(ident.getVar().getValue(), expr);
    }

    /**
     * Remove a symbol and its definition expression into the scope
     *
     * @param ident
     */
    public void removeSymbolExpressionMapping(VariableExpr ident) {
        if (ident == null) {
            return;
        }
        varExprMap.remove(ident.getVar().getValue());
    }

    /**
     * @return the variable substituion environment for inlining variable references by its original
     */
    public VariableSubstitutionEnvironment getVarSubstitutionEnvironment() {
        VariableSubstitutionEnvironment env = new VariableSubstitutionEnvironment();
        env.addMappings(varExprMap);
        return env;
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

    /**
     * Retrieve all the visible symbols in the current scope.
     *
     * @return an iterator of visible symbols.
     */
    public Iterator<Identifier> liveSymbols() {
        final Iterator<Identifier> identifierIterator = symbols.values().iterator();
        final Iterator<Identifier> parentIterator = parent == null ? null : parent.liveSymbols();
        return new Iterator<Identifier>() {
            private Identifier currentSymbol = null;

            @Override
            public boolean hasNext() {
                currentSymbol = null;
                if (identifierIterator != null && identifierIterator.hasNext()) {
                    currentSymbol = identifierIterator.next();
                } else if (!maskParentScope && parentIterator != null && parentIterator.hasNext()) {
                    do {
                        Identifier symbolFromParent = parentIterator.next();
                        if (!symbols.containsKey(symbolFromParent.getValue())) {
                            currentSymbol = symbolFromParent;
                            break;
                        }
                    } while (parentIterator.hasNext());
                }

                // Return true if currentSymbol is set.
                if (currentSymbol == null) {
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public Identifier next() {
                if (currentSymbol == null) {
                    throw new IllegalStateException(
                            "Please make sure that hasNext() returns true before calling next().");
                } else {
                    return currentSymbol;
                }
            }

        };
    }
}
