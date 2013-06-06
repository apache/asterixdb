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
package edu.uci.ics.asterix.aql.parser;

import java.util.Stack;

import edu.uci.ics.asterix.aql.context.Scope;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.Counter;

public abstract class ScopeChecker {

    protected Counter varCounter = new Counter(-1);

    protected Stack<Scope> scopeStack = new Stack<Scope>();

    protected Stack<Scope> forbiddenScopeStack = new Stack<Scope>();

    protected String[] inputLines;

    protected String defaultDataverse;

    protected void setInput(String s) {
        inputLines = s.split("\n");
    }

    // Forbidden scopes are used to disallow, in a limit clause, variables
    // having the same name as a variable defined by the FLWOR in which that
    // limit clause appears.

    /**
     * Create a new scope, using the top scope in scopeStack as parent scope
     * 
     * @param scopeStack
     * @return new scope
     */
    public final Scope createNewScope() {
        Scope scope = new Scope(this, scopeStack.peek());// top one as parent
        scopeStack.push(scope);
        return scope;
    }

    /**
     * Extend the current scope
     * 
     * @param scopeStack
     * @return
     */
    public final Scope extendCurrentScope() {
        return extendCurrentScope(false);
    }

    public final Scope extendCurrentScope(boolean maskParentScope) {
        Scope scope = extendCurrentScopeNoPush(maskParentScope);
        scopeStack.pop();
        scopeStack.push(scope);
        return scope;
    }

    public final Scope extendCurrentScopeNoPush(boolean maskParentScope) {
        Scope scope = scopeStack.peek();
        scope = new Scope(this, scope, maskParentScope);
        return scope;
    }

    public final void replaceCurrentScope(Scope scope) {
        scopeStack.pop();
        scopeStack.push(scope);
    }

    /**
     * Remove current scope
     * 
     * @return
     */
    public final Scope removeCurrentScope() {
        return scopeStack.pop();
    }

    /**
     * get current scope
     * 
     * @return
     */
    public final Scope getCurrentScope() {
        return scopeStack.peek();
    }

    /**
     * find symbol in the scope
     * 
     * @return identifier
     */
    public final Identifier lookupSymbol(String name) {
        if (name != null) {
            return getCurrentScope().findSymbol(name);
        } else {
            return null;
        }
    }

    /**
     * find FunctionSignature in the scope
     * 
     * @return functionDescriptor
     */
    public final FunctionSignature lookupFunctionSignature(String dataverse, String name, int arity) {
        if (dataverse != null) {
            return getCurrentScope().findFunctionSignature(dataverse, name, arity);
        } else {
            return null;
        }
    }

    public final int getVarCounter() {
        return varCounter.get();
    }

    public final void setVarCounter(Counter varCounter) {
        this.varCounter = varCounter;
    }

    public final void incVarCounter() {
        varCounter.inc();
    }

    public final void pushForbiddenScope(Scope s) {
        forbiddenScopeStack.push(s);
    }

    public final void popForbiddenScope() {
        forbiddenScopeStack.pop();
    }

    public final boolean isInForbiddenScopes(String ident) {
        for (Scope s : forbiddenScopeStack) {
            if (s.findLocalSymbol(ident) != null) {
                return true;
            }
        }
        return false;
    }

    public static final String removeQuotesAndEscapes(String s) {
        char q = s.charAt(0); // simple or double quote
        String stripped = s.substring(1, s.length() - 1);
        return stripped.replaceAll("\\\\" + q, "\\" + q);
    }

    public String extractFragment(int beginLine, int beginColumn, int endLine, int endColumn) {
        StringBuilder extract = new StringBuilder();
        extract.append(inputLines[beginLine - 1].trim().length() > 1 ? inputLines[beginLine - 1].trim().substring(beginColumn)
                : "");
        for (int i = beginLine + 1; i < endLine; i++) {
            extract.append("\n");
            extract.append(inputLines[i - 1]);
        }
        extract.append("\n");
        extract.append(inputLines[endLine - 1].substring(0, endColumn - 1));
        return extract.toString().trim();
    }

}
