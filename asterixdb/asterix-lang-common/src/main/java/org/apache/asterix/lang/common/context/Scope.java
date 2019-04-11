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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.expression.VariableExpr;
import org.apache.asterix.lang.common.parser.ScopeChecker;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.commons.collections4.iterators.ReverseListIterator;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.SourceLocation;

public final class Scope {
    private final ScopeChecker scopeChecker;
    private final Scope parent;
    private final LinkedHashMap<String, Pair<Identifier, Set<? extends SymbolAnnotation>>> symbols;
    private final boolean maskParentScope;
    private FunctionSignatures functionSignatures;

    public Scope(ScopeChecker scopeChecker) {
        this(scopeChecker, null);
    }

    public Scope(ScopeChecker scopeChecker, Scope parent) {
        this(scopeChecker, parent, false);
    }

    public Scope(ScopeChecker scopeChecker, Scope parent, boolean maskParentScope) {
        this.scopeChecker = scopeChecker;
        this.parent = parent;
        this.maskParentScope = maskParentScope;
        this.symbols = new LinkedHashMap<>();
    }

    /**
     * Find a symbol in the scope
     *
     * @param name
     * @return the Identifier of this symbol; otherwise null;
     */
    public Pair<Identifier, Set<? extends SymbolAnnotation>> findSymbol(String name) {
        Pair<Identifier, Set<? extends SymbolAnnotation>> symbol = symbols.get(name);
        if (symbol == null && !maskParentScope && parent != null) {
            symbol = parent.findSymbol(name);
        }
        return symbol;
    }

    public Pair<Identifier, Set<? extends SymbolAnnotation>> findLocalSymbol(String name) {
        return symbols.get(name);
    }

    /**
     * Add a symbol into scope
     *
     * @param ident
     */
    public void addSymbolToScope(Identifier ident) {
        addSymbolToScope(ident, null);
    }

    public void addSymbolToScope(Identifier ident, Set<? extends SymbolAnnotation> annotations) {
        if (annotations == null) {
            annotations = Collections.emptySet();
        }
        symbols.put(ident.getValue(), new Pair<>(ident, annotations));
    }

    public void addNewVarSymbolToScope(VarIdentifier ident) {
        addNewVarSymbolToScope(ident, null);
    }

    public void addNewVarSymbolToScope(VarIdentifier ident, Set<? extends SymbolAnnotation> annotations) {
        scopeChecker.incVarCounter();
        ident.setId(scopeChecker.getVarCounter());
        addSymbolToScope(ident, annotations);
    }

    /**
     * Add a FunctionDescriptor into functionSignatures
     *
     * @param signature
     *            FunctionSignature
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
     * Merge yet-another Scope instance into the current Scope instance.
     *
     * @param scope
     *            the other Scope instance.
     */
    public void merge(Scope scope) {
        symbols.putAll(scope.symbols);
        if (functionSignatures != null && scope.functionSignatures != null) {
            functionSignatures.addAll(scope.functionSignatures);
        }
    }

    /**
     * Retrieve all the visible symbols in the current scope.
     *
     * @return an iterator of visible symbols.
     */
    public Iterator<Pair<Identifier, Set<? extends SymbolAnnotation>>> liveSymbols(Scope stopAtExclusive) {
        final Iterator<Pair<Identifier, Set<? extends SymbolAnnotation>>> identifierIterator =
                new ReverseListIterator<>(new ArrayList<>(symbols.values()));
        final Iterator<Pair<Identifier, Set<? extends SymbolAnnotation>>> parentIterator =
                parent == null || parent == stopAtExclusive ? null : parent.liveSymbols(stopAtExclusive);
        return new Iterator<Pair<Identifier, Set<? extends SymbolAnnotation>>>() {
            private Pair<Identifier, Set<? extends SymbolAnnotation>> currentSymbol = null;

            @Override
            public boolean hasNext() {
                currentSymbol = null;
                if (identifierIterator.hasNext()) {
                    currentSymbol = identifierIterator.next();
                } else if (!maskParentScope && parentIterator != null && parentIterator.hasNext()) {
                    do {
                        Pair<Identifier, Set<? extends SymbolAnnotation>> symbolFromParent = parentIterator.next();
                        if (!symbols.containsKey(symbolFromParent.first.getValue())) {
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
            public Pair<Identifier, Set<? extends SymbolAnnotation>> next() {
                if (currentSymbol == null) {
                    throw new IllegalStateException(
                            "Please make sure that hasNext() returns true before calling next().");
                } else {
                    return currentSymbol;
                }
            }

        };
    }

    public Map<VariableExpr, Set<? extends SymbolAnnotation>> getLiveVariables() {
        return getLiveVariables(null);
    }

    public Map<VariableExpr, Set<? extends SymbolAnnotation>> getLiveVariables(Scope stopAtExclusive) {
        LinkedHashMap<VariableExpr, Set<? extends SymbolAnnotation>> vars = new LinkedHashMap<>();
        Iterator<Pair<Identifier, Set<? extends SymbolAnnotation>>> symbolIterator = liveSymbols(stopAtExclusive);
        while (symbolIterator.hasNext()) {
            Pair<Identifier, Set<? extends SymbolAnnotation>> p = symbolIterator.next();
            Identifier identifier = p.first;
            if (identifier instanceof VarIdentifier) {
                VarIdentifier varId = (VarIdentifier) identifier;
                vars.put(new VariableExpr(varId), p.second);
            }
        }
        return vars;
    }

    public static Set<VariableExpr> findVariablesAnnotatedBy(Set<VariableExpr> candidateVars,
            SymbolAnnotation annotationTest, Map<VariableExpr, Set<? extends SymbolAnnotation>> annotationMap,
            SourceLocation sourceLoc) throws CompilationException {
        Set<VariableExpr> resultVars = new HashSet<>();
        for (VariableExpr var : candidateVars) {
            Set<? extends SymbolAnnotation> varAnnotations = annotationMap.get(var);
            if (varAnnotations == null) {
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, sourceLoc);
            }
            if (varAnnotations.contains(annotationTest)) {
                resultVars.add(var);
            }
        }
        return resultVars;
    }

    // Returns local symbols within the current scope.
    public Set<String> getLocalSymbols() {
        return symbols.keySet();
    }

    public Scope getParentScope() {
        return parent;
    }

    public interface SymbolAnnotation {
    }
}
