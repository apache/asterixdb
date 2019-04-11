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
package org.apache.asterix.lang.common.parser;

import java.util.Set;
import java.util.Stack;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.lang.common.context.RootScopeFactory;
import org.apache.asterix.lang.common.context.Scope;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;

public class ScopeChecker {

    protected static String quot = "\"";

    protected String eol = System.getProperty("line.separator", "\n");

    protected Counter varCounter = new Counter(-1);

    protected Stack<Scope> scopeStack = new Stack<>();

    protected Stack<Scope> forbiddenScopeStack = new Stack<>();

    protected String[] inputLines;

    protected String defaultDataverse;

    public ScopeChecker() {
        scopeStack.push(RootScopeFactory.createRootScope(this));
    }

    protected void setInput(String s) {
        inputLines = s.split("\n|\r\n?");
    }

    // Forbidden scopes are used to disallow, in a limit clause, variables
    // having the same name as a variable defined by the FLWOR in which that
    // limit clause appears.

    /**
     * Create a new scope, using the top scope in scopeStack as parent scope
     *
     * @return new scope
     */
    public final Scope createNewScope() {
        Scope scope = extendCurrentScopeNoPush(false);
        scopeStack.push(scope);
        return scope;
    }

    /**
     * Extend the current scope
     *
     * @return
     */
    public final Scope extendCurrentScope() {
        Scope scope = extendCurrentScopeNoPush(false);
        replaceCurrentScope(scope);
        return scope;
    }

    protected final Scope extendCurrentScopeNoPush(boolean maskParentScope) {
        Scope parent = scopeStack.peek();
        return new Scope(this, parent, maskParentScope);
    }

    public final void replaceCurrentScope(Scope scope) {
        scopeStack.pop();
        scopeStack.push(scope);
    }

    public final void pushExistingScope(Scope scope) {
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
     * get scope preceding the current scope
     * @return preceding scope or {@code null} if current scope is the top one
     */
    public final Scope getPrecedingScope() {
        int n = scopeStack.size();
        return n > 1 ? scopeStack.get(n - 2) : null;
    }

    /**
     * find symbol in the scope
     *
     * @return identifier
     */
    public final Identifier lookupSymbol(String name) {
        if (name != null) {
            Pair<Identifier, Set<? extends Scope.SymbolAnnotation>> symbol = getCurrentScope().findSymbol(name);
            if (symbol != null) {
                return symbol.first;
            }
        }
        return null;
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

    protected int appendExpected(StringBuilder expected, int[][] expectedTokenSequences, String[] tokenImage) {
        int maxSize = 0;
        for (int i = 0; i < expectedTokenSequences.length; i++) {
            if (maxSize < expectedTokenSequences[i].length) {
                maxSize = expectedTokenSequences[i].length;
            }
            for (int j = 0; j < expectedTokenSequences[i].length; j++) {
                append(expected, fixQuotes(tokenImage[expectedTokenSequences[i][j]]));
                append(expected, " ");
            }
            if (expectedTokenSequences[i][expectedTokenSequences[i].length - 1] != 0) {
                append(expected, "...");
            }
            append(expected, eol);
            append(expected, "    ");
        }
        return maxSize;
    }

    private void append(StringBuilder expected, String str) {
        if (expected != null) {
            expected.append(str);
        }
    }

    protected static String fixQuotes(String token) {
        final String stripped = stripQuotes(token);
        return stripped != null ? "'" + stripped + "'" : token;
    }

    protected static String stripQuotes(String token) {
        final int last = token.length() - 1;
        return token.charAt(0) == '"' && token.charAt(last) == '"' ? token.substring(1, last) : null;
    }

    protected static String addEscapes(String str) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            appendChar(escaped, str.charAt(i));
        }
        return escaped.toString();
    }

    private static void appendChar(StringBuilder escaped, char c) {
        char ch;
        switch (c) {
            case 0:
                return;
            case '\b':
                escaped.append("\\b");
                return;
            case '\t':
                escaped.append("\\t");
                return;
            case '\n':
                escaped.append("\\n");
                return;
            case '\f':
                escaped.append("\\f");
                return;
            case '\r':
                escaped.append("\\r");
                return;
            case '\"':
                escaped.append("\\\"");
                return;
            case '\'':
                escaped.append("\\\'");
                return;
            case '\\':
                escaped.append("\\\\");
                return;
            default:
                if ((ch = c) < 0x20 || ch > 0x7e) {
                    String s = "0000" + Integer.toString(ch, 16);
                    escaped.append("\\u").append(s.substring(s.length() - 4, s.length()));
                } else {
                    escaped.append(ch);
                }
        }
    }

    public static String removeQuotesAndEscapes(String s) {
        char q = s.charAt(0); // simple or double quote
        String stripped = s.substring(1, s.length() - 1);
        int pos = stripped.indexOf('\\');
        if (pos < 0) {
            return stripped;
        }
        StringBuilder res = new StringBuilder();
        int start = 0;
        while (pos >= 0) {
            res.append(stripped.substring(start, pos));
            char c = stripped.charAt(pos + 1);
            switch (c) {
                case '/':
                case '\\':
                    res.append(c);
                    break;
                case 'b':
                    res.append('\b');
                    break;
                case 'f':
                    res.append('\f');
                    break;
                case 'n':
                    res.append('\n');
                    break;
                case 'r':
                    res.append('\r');
                    break;
                case 't':
                    res.append('\t');
                    break;
                case '\'':
                case '"':
                    if (c == q) {
                        res.append(c);
                    }
                    break;
                default:
                    throw new IllegalStateException("'\\" + c + "' should have been caught by the lexer");
            }
            start = pos + 2;
            pos = stripped.indexOf('\\', start);
        }
        res.append(stripped.substring(start));
        return res.toString();
    }

    protected String getLine(int line) {
        int idx = line - 1;
        return idx >= 0 && idx < inputLines.length ? inputLines[idx] : "";
    }

    protected String extractFragment(int beginLine, int beginColumn, int endLine, int endColumn) {
        StringBuilder extract = new StringBuilder();
        if (beginLine == endLine) {
            // special case that we need to handle separately
            return inputLines[beginLine - 1].substring(beginColumn, endColumn - 1).trim();
        }
        extract.append(inputLines[beginLine - 1].substring(beginColumn));
        for (int i = beginLine + 1; i < endLine; i++) {
            extract.append("\n");
            extract.append(inputLines[i - 1]);
        }
        extract.append("\n");
        extract.append(inputLines[endLine - 1].substring(0, endColumn - 1));
        return extract.toString().trim();
    }
}
