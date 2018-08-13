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

import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A node of the expression tree, which may be a literal, attribute reference,
 * function call, classad, expression list, or an operator applied to other
 * ExprTree operands.
 */
public abstract class ExprTree {

    /// The kinds of nodes in expression trees
    public enum NodeKind {
        /// Literal node (string, integer, real, boolean, undefined, error)
        LITERAL_NODE,
        /// Attribute reference node (attr, .attr, expr.attr)
        ATTRREF_NODE,
        /// Expression operation node (unary, binary, ternary)/
        OP_NODE,
        /// Function call node
        FN_CALL_NODE,
        /// ClassAd node
        CLASSAD_NODE,
        /// Expression list node
        EXPR_LIST_NODE,
        /// Expression envelope.
        EXPR_ENVELOPE
    }

    public enum EvalResult {
        EVAL_FAIL,
        EVAL_OK,
        EVAL_UNDEF,
        EVAL_ERROR
    }

    public static final int EVAL_FAIL_Int = 0;
    public static final int EVAL_OK_Int = 1;
    public static final int EVAL_UNDEF_Int = 2;
    public static final int EVAL_ERROR_Int = 3;

    public static final int MAX_CLASSAD_RECURSION = 1000;

    public boolean isTreeHolder() {
        return false;
    }

    public int size;
    public ClassAd parentScope;

    private CallableDebugFunction userDebugFunction;
    protected final ClassAdObjectPool objectPool;

    public abstract void reset();

    public ExprTree(ClassAdObjectPool objectPool) {
        if (objectPool == null) {
            System.out.println();
        }
        this.objectPool = objectPool;
        this.parentScope = null;
        this.size = 0;
    }

    public ExprTree(ExprTree expr, ClassAdObjectPool objectPool) {
        if (objectPool == null) {
            System.out.println();
        }
        this.objectPool = objectPool;
        this.size = expr.size;
    }

    public void resetExprTree(ExprTree expr) {
        if (expr == null) {
            this.size = 0;
        } else {
            this.size = expr.size;
        }
    }

    public static class ExprHash {
        public static int call(ExprTree x) {
            return x.size;
        }
    }

    public ExprTree getTree() {
        return this;
    }

    /**
     * Sets the lexical parent scope of the expression, which is used to
     * determine the lexical scoping structure for resolving attribute
     * references. (However, the semantic parent may be different from
     * the lexical parent if a <tt>super</tt> attribute is specified.)
     * This method is automatically called when expressions are
     * inserted into ClassAds, and should thus be called explicitly
     * only when evaluating expressions which haven't been inserted
     * into a ClassAd.
     */
    public void setParentScope(ClassAd scope) {
        parentScope = scope;
        privateSetParentScope(scope);
    }

    abstract protected void privateSetParentScope(ClassAd scope);

    /**
     * Gets the parent scope of the expression.
     *
     * @return The parent scope of the expression.
     */
    public ClassAd getParentScope() {
        return parentScope;
    }

    /**
     * Makes a deep copy of the expression tree
     *
     * @return A deep copy of the expression, or NULL on failure.
     * @throws HyracksDataException
     */

    public abstract ExprTree copy() throws HyracksDataException;

    /**
     * Gets the node kind of this expression node.
     *
     * @return The node kind. Child nodes MUST implement this.
     * @see NodeKind
     */
    public abstract NodeKind getKind();

    /**
     * To eliminate the mass of external checks to see if the ExprTree is
     * a classad.
     */
    public static boolean isClassAd(Object o) {
        return (o instanceof ClassAd);
    }

    /**
     * Return a ptr to the raw exprtree below the interface
     */

    public ExprTree self() {
        return this;
    }

    /// A debugging method; send expression to stdout
    public void puke() throws HyracksDataException {
        PrettyPrint unp = objectPool.prettyPrintPool.get();
        AMutableCharArrayString buffer = objectPool.strPool.get();
        unp.unparse(buffer, this);
        System.out.println(buffer.toString());
    }

    //Pass in a pointer to a function taking a const char *, which will
    //print it out somewhere useful, when the classad debug() function
    //is called
    public void setUserDebugFunction(CallableDebugFunction dbf) {
        this.userDebugFunction = dbf;
    }

    public void debugPrint(String message) {
        if (userDebugFunction != null) {
            userDebugFunction.call(message);
        }
    }

    public void debugFormatValue(Value value) throws HyracksDataException {
        debugFormatValue(value, 0.0);
    }

    public void debugFormatValue(Value value, double time) throws HyracksDataException {
        MutableBoolean boolValue = objectPool.boolPool.get();
        AMutableInt64 intValue = objectPool.int64Pool.get();
        AMutableDouble doubleValue = objectPool.doublePool.get();
        AMutableCharArrayString stringValue = objectPool.strPool.get();

        if (NodeKind.CLASSAD_NODE == getKind()) {
            return;
        }

        PrettyPrint unp = objectPool.prettyPrintPool.get();
        AMutableCharArrayString buffer = objectPool.strPool.get();
        unp.unparse(buffer, this);

        String result = "Classad debug: ";
        if (time != 0) {
            String buf = String.format("%5.5fms", time * 1000);
            result += "[";
            result += buf;
            result += "] ";
        }
        result += buffer;
        result += " --> ";

        switch (value.getType()) {
            case NULL_VALUE:
                result += "NULL\n";
                break;
            case ERROR_VALUE:
                if ((NodeKind.FN_CALL_NODE == getKind()) && !((FunctionCall) (this)).functionIsDefined()) {
                    result += "ERROR (function is not defined)\n";
                } else {
                    result += "ERROR\n";
                }
                break;
            case UNDEFINED_VALUE:
                result += "UNDEFINED\n";
                break;
            case BOOLEAN_VALUE:
                if (value.isBooleanValue(boolValue)) {
                    result += boolValue.booleanValue() ? "TRUE\n" : "FALSE\n";
                }
                break;
            case INTEGER_VALUE:
                if (value.isIntegerValue(intValue)) {
                    result += String.format("%lld", intValue.getLongValue());
                    result += "\n";
                }
                break;

            case REAL_VALUE:
                if (value.isRealValue(doubleValue)) {
                    result += String.format("%lld", doubleValue.getDoubleValue());
                    result += "\n";
                }
                break;
            case RELATIVE_TIME_VALUE:
                result += "RELATIVE TIME\n";
                break;
            case ABSOLUTE_TIME_VALUE:
                result += "ABSOLUTE TIME\n";
                break;
            case STRING_VALUE:
                if (value.isStringValue(stringValue)) {
                    result += stringValue.toString();
                    result += "\n";
                }
                break;
            case CLASSAD_VALUE:
                result += "CLASSAD\n";
                break;
            case LIST_VALUE:
                result += "LIST\n";
                break;
            case SLIST_VALUE:
                result += "SLIST\n";
                break;
        }
        debugPrint(result);
    }

    /**
     * Evaluate this tree
     *
     * @param state
     *            The current state
     * @param val
     *            The result of the evaluation
     * @return true on success, false on failure
     * @throws HyracksDataException
     */

    public boolean publicEvaluate(EvalState state, Value val) throws HyracksDataException {
        return privateEvaluate(state, val);
    }

    public boolean publicEvaluate(EvalState state, Value val, ExprTreeHolder sig) throws HyracksDataException {
        return privateEvaluate(state, val, sig);
    }

    public abstract boolean privateEvaluate(EvalState state, Value val) throws HyracksDataException;

    public abstract boolean privateEvaluate(EvalState state, Value val, ExprTreeHolder tree)
            throws HyracksDataException;

    /**
     * Evaluate this tree.
     * This only works if the expression is currently part of a ClassAd.
     *
     * @param val
     *            The result of the evaluation
     * @return true on success, false on failure
     * @throws HyracksDataException
     */
    public boolean publicEvaluate(Value val) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        if (parentScope == null) {
            val.setErrorValue();
            return false;
        } else {
            state.setScopes(parentScope);
            return (publicEvaluate(state, val));
        }
    }

    /**
     * Is this ExprTree the same as the tree?
     *
     * @return true if it is the same, false otherwise
     */
    public abstract boolean sameAs(ExprTree tree);

    /**
     * Fill in this ExprTree with the contents of the other ExprTree.
     *
     * @return true if the copy succeeded, false otherwise.
     * @throws HyracksDataException
     */
    public void copyFrom(ExprTree tree) throws HyracksDataException {
        if (!this.equals(tree)) {
            parentScope = tree.parentScope;
        }
        return;
    }

    public interface CallableDebugFunction {
        public void call(String message);
    }

    public boolean publicEvaluate(Value val, ExprTreeHolder sig) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        state.setScopes(parentScope);
        return (publicEvaluate(state, val, sig));
    }

    public boolean publicFlatten(Value val, ExprTreeHolder tree) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        state.setScopes(parentScope);
        return (publicFlatten(state, val, tree));
    }

    public boolean publicFlatten(EvalState state, Value val, ExprTreeHolder tree, AMutableInt32 op)
            throws HyracksDataException {
        return (privateFlatten(state, val, tree, op));
    }

    public boolean publicFlatten(EvalState state, Value val, ExprTreeHolder tree) throws HyracksDataException {
        return (privateFlatten(state, val, tree, null));
    }

    public abstract boolean privateFlatten(EvalState state, Value val, ExprTreeHolder tree, AMutableInt32 op)
            throws HyracksDataException;

    public boolean isClassad(ClassAd ptr) {
        return (ptr instanceof ClassAd);
    }

    public int exprHash(ExprTree expr, int numBkts) {
        int result = expr.getKind().ordinal() + 1000;
        result += numBkts * (3 / 2);
        return (result % numBkts);
    }

    @Override
    public String toString() {
        ClassAdObjectPool objectPool = new ClassAdObjectPool();
        ClassAdUnParser unparser = new ClassAdUnParser(objectPool);
        AMutableCharArrayString string_representation = objectPool.strPool.get();

        try {
            unparser.unparse(string_representation, this);
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
        return string_representation.toString();

    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ExprTree) {
            return sameAs((ExprTree) o);
        }
        return false;
    }

    public int size() {
        return size;
    }
}
