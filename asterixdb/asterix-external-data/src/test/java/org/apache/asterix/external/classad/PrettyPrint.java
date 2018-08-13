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
package org.apache.asterix.external.classad;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.external.classad.ExprTree.NodeKind;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PrettyPrint extends ClassAdUnParser {
    private int classadIndent;
    private int listIndent;
    private boolean wantStringQuotes;
    private boolean minimalParens;
    private int indentLevel;

    public PrettyPrint(ClassAdObjectPool objectPool) {
        super(objectPool);
        classadIndent = 4;
        listIndent = 3;
        wantStringQuotes = true;
        minimalParens = false;
        indentLevel = 0;
    }

    /// Set the indentation width for displaying lists
    public void setListIndentation() {
        // default is 4
        setListIndentation(4);
    }

    public void setClassAdIndentation(int len) {
        classadIndent = len;
    }

    public int setClassAdIndentation() {
        return (classadIndent);
    }

    public void setListIndentation(int len) {
        listIndent = len;
    }

    public int getListIndentation() {
        return (listIndent);
    }

    public void setWantStringQuotes(boolean b) {
        wantStringQuotes = b;
    }

    public boolean getWantStringQuotes() {
        return (wantStringQuotes);
    }

    public void setMinimalParentheses(boolean b) {
        minimalParens = b;
    }

    public boolean getMinimalParentheses() {
        return (minimalParens);
    }

    @Override
    public void unparseAux(AMutableCharArrayString buffer, int op, ExprTreeHolder op1, ExprTreeHolder op2,
            ExprTreeHolder op3) throws HyracksDataException {
        if (!minimalParens) {
            super.unparseAux(buffer, op, op1, op2, op3);
            return;
        }

        // case 0: parentheses op
        if (op == Operation.OpKind_PARENTHESES_OP) {
            unparse(buffer, op1);
            return;
        }
        // case 1: check for unary ops
        if (op == Operation.OpKind_UNARY_PLUS_OP || op == Operation.OpKind_UNARY_MINUS_OP
                || op == Operation.OpKind_LOGICAL_NOT_OP || op == Operation.OpKind_BITWISE_NOT_OP) {
            buffer.appendString(opString[op]);
            unparse(buffer, op1);
            return;
        }
        // case 2: check for ternary op
        if (op == Operation.OpKind_TERNARY_OP) {
            unparse(buffer, op1);
            buffer.appendString(" ? ");
            unparse(buffer, op2);
            buffer.appendString(" : ");
            unparse(buffer, op3);
            return;
        }
        // case 3: check for subscript op
        if (op == Operation.OpKind_SUBSCRIPT_OP) {
            unparse(buffer, op1);
            buffer.appendChar('[');
            unparse(buffer, op2);
            buffer.appendChar(']');
            return;
        }
        // all others are binary ops
        AMutableInt32 top = objectPool.int32Pool.get();
        ExprTreeHolder t1 = objectPool.mutableExprPool.get(), t2 = objectPool.mutableExprPool.get(),
                t3 = objectPool.mutableExprPool.get();

        if (op1.getKind() == NodeKind.OP_NODE) {
            ((Operation) op1.getInnerTree()).getComponents(top, t1, t2, t3);
            if (Operation.precedenceLevel(top.getIntegerValue()) < Operation.precedenceLevel(op)) {
                buffer.appendString(" ( ");
                unparseAux(buffer, top.getIntegerValue(), t1, t2, t3);
                buffer.appendString(" ) ");
            }
        } else {
            unparse(buffer, op1);
        }
        buffer.appendString(opString[op]);
        if (op2.getKind() == NodeKind.OP_NODE) {
            ((Operation) op2.getInnerTree()).getComponents(top, t1, t2, t3);
            if (Operation.precedenceLevel(top.getIntegerValue()) < Operation.precedenceLevel(op)) {
                buffer.appendString(" ( ");
                unparseAux(buffer, top.getIntegerValue(), t1, t2, t3);
                buffer.appendString(" ) ");
            }
        } else {
            unparse(buffer, op2);
        }
    }

    @Override
    public void unparseAux(AMutableCharArrayString buffer, Map<CaseInsensitiveString, ExprTree> attrs)
            throws HyracksDataException {
        if (classadIndent > 0) {
            indentLevel += classadIndent;
            buffer.appendChar('\n');
            int i = 0;
            while (i < indentLevel) {
                buffer.appendChar(' ');
                i++;
            }
            buffer.appendChar('[');
            indentLevel += classadIndent;
        } else {
            buffer.appendString("[ ");
        }
        for (Entry<CaseInsensitiveString, ExprTree> entry : attrs.entrySet()) {
            if (classadIndent > 0) {
                buffer.appendChar('\n');
                int i = 0;
                while (i < indentLevel) {
                    buffer.appendChar(' ');
                    i++;
                }
            }
            super.unparseAux(buffer, entry.getKey().get());
            buffer.appendString(" = ");
            unparse(buffer, entry.getValue());
            buffer.appendString("; ");
        }
        if (buffer.charAt(buffer.getLength() - 2) == ';') {
            buffer.setLength(buffer.getLength() - 2);
        }
        if (classadIndent > 0) {
            indentLevel -= classadIndent;
            buffer.appendChar('\n');
            int i = 0;
            while (i < indentLevel) {
                buffer.appendChar(' ');
                i++;
            }
            buffer.appendChar(']');
            indentLevel -= classadIndent;
        } else {
            buffer.appendString(" ]");
        }
    }

    @Override
    public void unparseAux(AMutableCharArrayString buffer, ExprList exprs) throws HyracksDataException {
        if (listIndent > 0) {
            indentLevel += listIndent;
            buffer.appendChar('\n');
            int i = 0;
            while (i < indentLevel) {
                buffer.appendChar(' ');
                i++;
            }
            buffer.appendChar('{');
            indentLevel += listIndent;
        } else {
            buffer.appendString("{ ");
        }
        for (ExprTree itr : exprs.getExprList()) {
            if (listIndent > 0) {
                int i = 0;
                buffer.appendChar('\n');
                while (i < indentLevel) {
                    buffer.appendChar(' ');
                    i++;
                }
            }
            super.unparse(buffer, itr);
            buffer.appendChar(',');
        }
        if (exprs.size() > 0) {
            buffer.decrementLength();
        }
        if (listIndent > 0) {
            indentLevel -= listIndent;
            buffer.appendChar('\n');
            int i = 0;
            while (i < indentLevel) {
                buffer.appendChar(' ');
                i++;
            }
            buffer.appendChar('}');
            indentLevel -= listIndent;
        } else {
            buffer.appendString(" }");
        }
    }

    /* Checks whether string qualifies to be a non-quoted attribute */
    public static boolean identifierNeedsQuoting(String str) {
        boolean needs_quoting;
        // must start with [a-zA-Z_]
        if (!Character.isAlphabetic(str.charAt(0)) && str.charAt(0) != '_') {
            needs_quoting = true;
        } else {

            // all other characters must be [a-zA-Z0-9_]
            int i = 1;
            while (i < str.length() && (Character.isLetterOrDigit(str.charAt(i)) || str.charAt(i) == '_')) {
                i++;
            }
            // needs quoting if we found a special character
            // before the end of the string.
            needs_quoting = str.charAt(i - 1) != '\0';
        }
        return needs_quoting;
    }
}
