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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.external.classad.Value.NumberFactor;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ClassAdUnParser {

    // table of string representation of operators
    public static final String[] opString = { "", " < ", " <= ", " != ", " == ", " >= ", " > ", " is ", " isnt ", " +",
            " -", " + ", " - ", " * ", " / ", " % ", " !", " || ", " && ", " ~", " | ", " ^ ", " & ", " << ", " >> ",
            " >>> ", " () ", " [] ", " ?: " };
    protected static char delimiter = '\"';

    protected final ClassAdObjectPool objectPool;

    /// Constructor
    public ClassAdUnParser(ClassAdObjectPool objectPool) {
        this.objectPool = objectPool;
    }

    // The default delimiter for strings is '\"'
    // This can be changed to '\'' to unparse quoted attributes, with this function
    public void setDelimiter(char delim) {
        delimiter = delim;
    }

    /**
     * Unparse a value
     *
     * @param buffer
     *            The string to unparse to
     * @param val
     *            The value to unparse
     * @throws HyracksDataException
     */
    public void unparse(AMutableCharArrayString buffer, Value val) throws HyracksDataException {
        switch (val.getType()) {
            case NULL_VALUE:
                buffer.appendString("(null-value)");
                break;

            case STRING_VALUE: {
                AMutableCharArrayString s = objectPool.strPool.get();
                val.isStringValue(s);
                buffer.appendChar('"');
                for (int i = 0; i < s.getLength(); i++) {
                    char ch = s.charAt(i);
                    if (ch == delimiter) {
                        if (delimiter == '\"') {
                            buffer.appendString("\\\"");
                            continue;
                        } else {
                            buffer.appendString("\\\'");
                            continue;
                        }
                    }
                    switch (ch) {
                        case '\b':
                            buffer.appendString("\\b");
                            continue;
                        case '\f':
                            buffer.appendString("\\f");
                            continue;
                        case '\n':
                            buffer.appendString("\\n");
                            continue;
                        case '\r':
                            buffer.appendString("\\r");
                            continue;
                        case '\t':
                            buffer.appendString("\\t");
                            continue;
                        case '\\':
                            buffer.appendString("\\\\");
                            continue;
                        case '\'':
                            buffer.appendString("\'");
                            continue;
                        case '\"':
                            buffer.appendString("\"");
                            continue;
                        default:
                            if (Character.isISOControl(ch)) {
                                // print octal representation
                                buffer.appendString(String.format("\\%03o", ch));
                                continue;
                            }
                            break;
                    }

                    buffer.appendChar(ch);
                }
                buffer.appendChar('"');
                return;
            }
            case INTEGER_VALUE: {
                AMutableInt64 i = objectPool.int64Pool.get();
                val.isIntegerValue(i);
                buffer.appendString(String.valueOf(i.getLongValue()));
                return;
            }
            case REAL_VALUE: {
                AMutableDouble real = objectPool.doublePool.get();
                val.isRealValue(real);
                if (real.getDoubleValue() == 0.0) {
                    // It might be positive or negative and it's
                    // hard to tell. printf is good at telling though.
                    // We also want to print it with as few
                    // digits as possible, which is why we don't use the
                    // case below.
                    buffer.appendString(String.valueOf(real.getDoubleValue()));
                } else if (Util.isNan(real.getDoubleValue())) {
                    buffer.appendString("real(\"NaN\")");
                } else if (Util.isInf(real.getDoubleValue()) == -1) {
                    buffer.appendString("real(\"-INF\")");
                } else if (Util.isInf(real.getDoubleValue()) == 1) {
                    buffer.appendString("real(\"INF\")");
                } else {
                    buffer.appendString(String.format("%1.15E", real.getDoubleValue()));
                }
                return;
            }
            case BOOLEAN_VALUE: {
                MutableBoolean b = objectPool.boolPool.get();
                val.isBooleanValue(b);
                buffer.appendString(b.booleanValue() ? "true" : "false");
                return;
            }
            case UNDEFINED_VALUE: {
                buffer.appendString("undefined");
                return;
            }
            case ERROR_VALUE: {
                buffer.appendString("error");
                return;
            }
            case ABSOLUTE_TIME_VALUE: {
                ClassAdTime asecs = objectPool.classAdTimePool.get();
                val.isAbsoluteTimeValue(asecs);

                buffer.appendString("absTime(\"");
                Util.absTimeToString(asecs, buffer);
                buffer.appendString("\")");
                return;
            }
            case RELATIVE_TIME_VALUE: {
                ClassAdTime rsecs = objectPool.classAdTimePool.get();
                val.isRelativeTimeValue(rsecs);
                buffer.appendString("relTime(\"");
                Util.relTimeToString(rsecs.getRelativeTime(), buffer);
                buffer.appendString("\")");

                return;
            }
            case CLASSAD_VALUE: {
                ClassAd ad = objectPool.classAdPool.get();
                Map<CaseInsensitiveString, ExprTree> attrs = objectPool.strToExprPool.get();
                val.isClassAdValue(ad);
                ad.getComponents(attrs, objectPool);
                unparseAux(buffer, attrs);
                return;
            }
            case SLIST_VALUE:
            case LIST_VALUE: {
                ExprList el = objectPool.exprListPool.get();
                val.isListValue(el);
                unparseAux(buffer, el);
                return;
            }
        }
    }

    /**
     * Unparse an expression
     *
     * @param buffer
     *            The string to unparse to
     * @param expr
     *            The expression to unparse
     * @throws HyracksDataException
     */
    public void unparse(AMutableCharArrayString buffer, ExprTree tree) throws HyracksDataException {
        if (tree == null) {
            buffer.appendString("<error:null expr>");
            return;
        }

        switch (tree.getKind()) {
            case LITERAL_NODE: { // value
                Value val = objectPool.valuePool.get();
                AMutableNumberFactor factor = objectPool.numFactorPool.get();
                ((Literal) tree.getTree()).getComponents(val, factor);
                unparseAux(buffer, val, factor.getFactor());
                return;
            }

            case ATTRREF_NODE: { // string
                ExprTreeHolder expr = objectPool.mutableExprPool.get(); //needs initialization
                AMutableCharArrayString ref = objectPool.strPool.get();
                MutableBoolean absolute = objectPool.boolPool.get();
                ((AttributeReference) tree.getTree()).getComponents(expr, ref, absolute);
                unparseAux(buffer, expr, ref, absolute.booleanValue());
                return;
            }

            case OP_NODE: { //string
                AMutableInt32 op = objectPool.int32Pool.get();
                ExprTreeHolder t1 = objectPool.mutableExprPool.get();
                ExprTreeHolder t2 = objectPool.mutableExprPool.get();
                ExprTreeHolder t3 = objectPool.mutableExprPool.get();
                ((Operation) tree.getTree()).getComponents(op, t1, t2, t3);
                unparseAux(buffer, op.getIntegerValue(), t1, t2, t3);
                return;
            }

            case FN_CALL_NODE: { // string
                AMutableCharArrayString fnName = objectPool.strPool.get();
                ExprList args = objectPool.exprListPool.get();
                ((FunctionCall) tree.getTree()).getComponents(fnName, args);
                unparseAux(buffer, fnName, args);
                return;
            }

            case CLASSAD_NODE: { // nested record
                Map<CaseInsensitiveString, ExprTree> attrs = objectPool.strToExprPool.get();
                ((ClassAd) tree.getTree()).getComponents(attrs, objectPool);
                unparseAux(buffer, attrs);
                return;
            }
            case EXPR_LIST_NODE: { // list
                ExprList exprs = objectPool.exprListPool.get();
                ((ExprList) tree.getTree()).getComponents(exprs);
                unparseAux(buffer, exprs);
                return;
            }

            default:
                // I really wonder whether we should except here, but I
                // don't want to do that without further consultation.
                // wenger 2003-12-11.
                buffer.setValue("");
                throw new HyracksDataException("unknown expression type");
        }
    }

    private void unparseAux(AMutableCharArrayString buffer, AMutableCharArrayString fnName, ExprList args)
            throws HyracksDataException {
        buffer.appendString(fnName);
        buffer.appendChar('(');
        for (ExprTree tree : args.getExprList()) {
            unparse(buffer, tree);
            buffer.appendChar(',');
        }
        if (args.size() > 0) {
            buffer.decrementLength();
        }
        buffer.appendChar(')');

    }

    public void unparseAux(AMutableCharArrayString buffer, final Value value, NumberFactor numFactor)
            throws HyracksDataException {
        unparse(buffer, value);
        if ((value.isIntegerValue() || value.isRealValue()) && numFactor != NumberFactor.NO_FACTOR) {
            buffer.appendChar((numFactor == NumberFactor.B_FACTOR) ? 'B'
                    : (numFactor == NumberFactor.K_FACTOR) ? 'K'
                            : (numFactor == NumberFactor.M_FACTOR) ? 'M'
                                    : (numFactor == NumberFactor.G_FACTOR) ? 'G'
                                            : (numFactor == NumberFactor.T_FACTOR) ? 'T' : '?');
            if (buffer.charAt(buffer.getLength() - 1) == '?') {
                buffer.reset();
                throw new HyracksDataException("bad number factor");
            }
        }
        return;
    }

    /**
     * @param buffer
     * @param tree
     * @param ref
     * @param absolute
     *            = false if ommitted
     * @throws HyracksDataException
     */
    public void unparseAux(AMutableCharArrayString buffer, final ExprTree tree, AMutableCharArrayString ref,
            boolean absolute) throws HyracksDataException {

        if (tree != null && tree.self() != null) {
            unparse(buffer, tree);
            buffer.appendChar('.');
            buffer.appendString(ref);
            return;
        }
        if (absolute) {
            buffer.appendChar('.');
        }
        unparseAux(buffer, ref);
    }

    public void unparseAux(AMutableCharArrayString buffer, final ExprTree tree, AMutableCharArrayString ref)
            throws HyracksDataException {
        unparseAux(buffer, tree, ref, false);
    }

    public void unparseAuxPairs(AMutableCharArrayString buffer, List<Entry<AMutableCharArrayString, ExprTree>> attrlist)
            throws HyracksDataException {
        String delim = "; "; // NAC
        buffer.appendString("[ ");
        for (Entry<AMutableCharArrayString, ExprTree> entry : attrlist) {
            unparseAux(buffer, entry.getKey());
            buffer.appendString(" = ");
            unparse(buffer, entry.getValue());
            buffer.appendString(delim);
        }
        //get rid of last delimiter
        buffer.setLength(buffer.getLength() - delim.length());
        buffer.appendString(" ]");
    }

    // to unparse attribute names (quoted & unquoted attributes)
    public void unparseAux(AMutableCharArrayString buffer, AMutableCharArrayString identifier)
            throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        AMutableCharArrayString idstr = objectPool.strPool.get();

        val.setStringValue(identifier);
        setDelimiter('\''); // change the delimiter from string-literal mode to quoted attribute mode
        unparse(idstr, val);
        setDelimiter('\"'); // set delimiter back to default setting
        idstr.erase(0, 1);
        idstr.erase(idstr.length() - 1, 1);
        if (identifierNeedsQuoting(idstr)) {
            idstr.insert(0, "'");
            idstr.appendString("'");
        }
        buffer.appendString(idstr);
    }

    static boolean identifierNeedsQuoting(AMutableCharArrayString aString) {
        return false;
    }

    public void unparseAux(AMutableCharArrayString buffer, Value val, AMutableNumberFactor factor)
            throws HyracksDataException {
        unparse(buffer, val);
        if ((val.isIntegerValue() || val.isRealValue()) && factor.getFactor() != NumberFactor.NO_FACTOR) {
            buffer.appendString((factor.getFactor() == NumberFactor.B_FACTOR) ? "B"
                    : (factor.getFactor() == NumberFactor.K_FACTOR) ? "K"
                            : (factor.getFactor() == NumberFactor.M_FACTOR) ? "M"
                                    : (factor.getFactor() == NumberFactor.G_FACTOR) ? "G"
                                            : (factor.getFactor() == NumberFactor.T_FACTOR) ? "T"
                                                    : "<error:bad factor>");
        }
        return;
    }

    public void unparseAux(AMutableCharArrayString buffer, ExprTree expr, String attrName, boolean absolute)
            throws HyracksDataException {
        if (expr != null) {
            unparse(buffer, expr);
            buffer.appendString("." + attrName);
            return;
        }
        if (absolute) {
            buffer.appendChar('.');
        }
        unparseAux(buffer, attrName);
    }

    public void unparseAux(AMutableCharArrayString buffer, int op, ExprTreeHolder t1, ExprTreeHolder t2,
            ExprTreeHolder t3) throws HyracksDataException {
        // case 0: parentheses op
        if (op == Operation.OpKind_PARENTHESES_OP) {
            buffer.appendString("( ");
            unparse(buffer, t1);
            buffer.appendString(" )");
            return;
        }
        // case 1: check for unary ops
        if (op == Operation.OpKind_UNARY_PLUS_OP || op == Operation.OpKind_UNARY_MINUS_OP
                || op == Operation.OpKind_LOGICAL_NOT_OP || op == Operation.OpKind_BITWISE_NOT_OP) {
            buffer.appendString(opString[op]);
            unparse(buffer, t1);
            return;
        }
        // case 2: check for ternary op
        if (op == Operation.OpKind_TERNARY_OP) {
            unparse(buffer, t1);
            buffer.appendString(" ? ");
            unparse(buffer, t2);
            buffer.appendString(" : ");
            unparse(buffer, t3);
            return;
        }
        // case 3: check for subscript op
        if (op == Operation.OpKind_SUBSCRIPT_OP) {
            unparse(buffer, t1);
            buffer.appendChar('[');
            unparse(buffer, t2);
            buffer.appendChar(']');
            return;
        }

        // all others are binary ops
        unparse(buffer, t1);
        buffer.appendString(opString[op]);
        unparse(buffer, t2);
    }

    public void UnparseAux(AMutableCharArrayString buffer, String fnName, ExprList args) throws HyracksDataException {
        buffer.appendString(fnName + "(");
        for (ExprTree tree : args.getExprList()) {
            unparse(buffer, tree);
            buffer.appendChar(',');
        }
        buffer.setChar(buffer.getLength() - 1, ')');
    }

    public void unparseAux(AMutableCharArrayString buffer, Map<CaseInsensitiveString, ExprTree> attrs)
            throws HyracksDataException {

        String delim = "; "; // NAC

        buffer.appendString("[ ");

        for (Entry<CaseInsensitiveString, ExprTree> entry : attrs.entrySet()) {
            unparseAux(buffer, entry.getKey().get());
            buffer.appendString(" = ");
            unparse(buffer, entry.getValue());
            buffer.appendString(delim); // NAC
        }
        buffer.setLength(buffer.getLength() - delim.length());
        buffer.appendString(" ]");

    }

    public void unparseAux(AMutableCharArrayString buffer, ExprList exprs) throws HyracksDataException {

        buffer.appendString("{ ");
        for (ExprTree expr : exprs.getExprList()) {
            unparse(buffer, expr);
            buffer.appendChar(',');
        }
        buffer.decrementLength();
        buffer.appendString(" }");
    }

    /* To unparse the identifier strings
     * based on the character content,
     * it's unparsed either as a quoted attribute or non-quoted attribute
     */
    public void unparseAux(AMutableCharArrayString buffer, String identifier) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        AMutableCharArrayString idstr = objectPool.strPool.get();

        val.setStringValue(identifier);
        setDelimiter('\''); // change the delimiter from string-literal mode to quoted attribute mode
        unparse(idstr, val);
        setDelimiter('\"'); // set delimiter back to default setting
        idstr.erase(0, 1);
        idstr.erase(idstr.length() - 1, 1);
        if (identifierNeedsQuoting(idstr)) {
            idstr.prependChar('\'');
            idstr.appendChar('\'');
        }
        buffer.appendString(idstr);
    }
}
