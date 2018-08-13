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

import org.apache.asterix.external.classad.Value.ValueType;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Represents a node of the expression tree which is an operation applied to
 * expression operands, like 3 + 2
 */
public class Operation extends ExprTree {
    enum SigValues {
        SIG_NONE,
        SIG_CHLD1,
        SIG_CHLD2,
        SIG_DUMMY,
        SIG_CHLD3
    }

    /// List of supported operators
    public static final int OpKind_NO_OP = 0;
    public static final int OpKind_FIRST_OP = 1;
    // Comparison
    public static final int OpKind_COMPARISON_START = OpKind_FIRST_OP;
    /** @name Strict comparison operators */
    public static final int OpKind_LESS_THAN_OP = OpKind_COMPARISON_START;
    public static final int OpKind_LESS_OR_EQUAL_OP = OpKind_LESS_THAN_OP + 1;
    public static final int OpKind_NOT_EQUAL_OP = OpKind_LESS_OR_EQUAL_OP + 1;
    public static final int OpKind_EQUAL_OP = OpKind_NOT_EQUAL_OP + 1;
    public static final int OpKind_GREATER_OR_EQUAL_OP = OpKind_EQUAL_OP + 1;
    public static final int OpKind_GREATER_THAN_OP = OpKind_GREATER_OR_EQUAL_OP + 1;
    /** @name Non-strict comparison operators */
    public static final int OpKind_META_EQUAL_OP = OpKind_GREATER_THAN_OP + 1;
    public static final int OpKind_IS_OP = OpKind_META_EQUAL_OP;
    public static final int OpKind_META_NOT_EQUAL_OP = OpKind_IS_OP + 1;
    public static final int OpKind_ISNT_OP = OpKind_META_NOT_EQUAL_OP;
    public static final int OpKind_COMPARISON_END = OpKind_ISNT_OP;
    /** @name Arithmetic operators */
    public static final int OpKind_ARITHMETIC_START = OpKind_COMPARISON_END + 1;
    public static final int OpKind_UNARY_PLUS_OP = OpKind_ARITHMETIC_START;
    public static final int OpKind_UNARY_MINUS_OP = OpKind_UNARY_PLUS_OP + 1;
    public static final int OpKind_ADDITION_OP = OpKind_UNARY_MINUS_OP + 1;
    public static final int OpKind_SUBTRACTION_OP = OpKind_ADDITION_OP + 1;
    public static final int OpKind_MULTIPLICATION_OP = OpKind_SUBTRACTION_OP + 1;
    public static final int OpKind_DIVISION_OP = OpKind_MULTIPLICATION_OP + 1;
    public static final int OpKind_MODULUS_OP = OpKind_DIVISION_OP + 1;
    public static final int OpKind_ARITHMETIC_END = OpKind_MODULUS_OP;
    /** @name Logical operators */
    public static final int OpKind_LOGIC_START = OpKind_ARITHMETIC_END + 1;
    public static final int OpKind_LOGICAL_NOT_OP = OpKind_LOGIC_START;
    public static final int OpKind_LOGICAL_OR_OP = OpKind_LOGICAL_NOT_OP + 1;
    public static final int OpKind_LOGICAL_AND_OP = OpKind_LOGICAL_OR_OP + 1;
    public static final int OpKind_LOGIC_END = OpKind_LOGICAL_AND_OP;
    /** @name Bitwise operators */
    public static final int OpKind_BITWISE_START = OpKind_LOGIC_END + 1;
    public static final int OpKind_BITWISE_NOT_OP = OpKind_BITWISE_START;
    public static final int OpKind_BITWISE_OR_OP = OpKind_BITWISE_NOT_OP + 1;
    public static final int OpKind_BITWISE_XOR_OP = OpKind_BITWISE_OR_OP + 1;
    public static final int OpKind_BITWISE_AND_OP = OpKind_BITWISE_XOR_OP + 1;
    public static final int OpKind_LEFT_SHIFT_OP = OpKind_BITWISE_AND_OP + 1;
    public static final int OpKind_RIGHT_SHIFT_OP = OpKind_LEFT_SHIFT_OP + 1;
    public static final int OpKind_URIGHT_SHIFT_OP = OpKind_RIGHT_SHIFT_OP + 1;
    public static final int OpKind_BITWISE_END = OpKind_URIGHT_SHIFT_OP;
    /** @name Miscellaneous operators */
    public static final int OpKind_MISC_START = OpKind_BITWISE_END + 1;
    public static final int OpKind_PARENTHESES_OP = OpKind_MISC_START;
    public static final int OpKind_SUBSCRIPT_OP = OpKind_PARENTHESES_OP + 1;
    public static final int OpKind_TERNARY_OP = OpKind_SUBSCRIPT_OP + 1;
    public static final int OpKind_MISC_END = OpKind_TERNARY_OP;
    public static final int OpKind_LAST_OP = OpKind_MISC_END;

    private int opKind;
    private final ExprTreeHolder child1;
    private final ExprTreeHolder child2;
    private final ExprTreeHolder child3;

    /// node type
    @Override
    public NodeKind getKind() {
        return NodeKind.OP_NODE;
    }

    public int getOpKind() {
        return opKind;
    }

    /**
     * Factory method to create an operation expression node
     *
     * @param kind
     *            The kind of operation.
     * @param e1
     *            The first sub-expression child of the node.
     * @param e2
     *            The second sub-expression child of the node (if any).
     * @param e3
     *            The third sub-expression child of the node (if any).
     * @return The constructed operation
     * @throws HyracksDataException
     */

    public static Operation createOperation(int opkind, ExprTree e1, ExprTree e2, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        return createOperation(opkind, e1, e2, null, objectPool);
    }

    public static Operation createOperation(int opkind, ExprTree e1, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        return createOperation(opkind, e1, null, null, objectPool);
    }

    // public access to operation function
    /**
     * Convenience method which operates on binary operators.
     *
     * @param op
     *            The kind of operation.
     * @param op1
     *            The first operand.
     * @param op2
     *            The second operand.
     * @param result
     *            The result of the operation.
     * @see OpKind, Value
     */

    /**
     * Convenience method which operates on ternary operators.
     *
     * @param op
     *            The kind of operation.
     * @param op1
     *            The first operand.
     * @param op2
     *            The second operand.
     * @param op3
     *            The third operand.
     * @param result
     *            The result of the operation.
     * @see OpKind, Value
     */

    /**
     * Predicate which tests if an operator is strict.
     *
     * @param op
     *            The operator to be tested.
     * @return true if the operator is strict, false otherwise.
     */

    public Operation(ClassAdObjectPool objectPool) {
        super(objectPool);
        opKind = OpKind_NO_OP;
        child1 = new ExprTreeHolder(objectPool);
        child2 = new ExprTreeHolder(objectPool);
        child3 = new ExprTreeHolder(objectPool);
    }

    public Operation(Operation op, ClassAdObjectPool objectPool) throws HyracksDataException {
        super(objectPool);
        child1 = new ExprTreeHolder(objectPool);
        child2 = new ExprTreeHolder(objectPool);
        child3 = new ExprTreeHolder(objectPool);
        copyFrom(op);
    }

    @Override
    public ExprTree copy() throws HyracksDataException {
        Operation newTree = objectPool.operationPool.get();
        newTree.copyFrom(this);
        return newTree;
    }

    public boolean copyFrom(Operation op) throws HyracksDataException {
        child1.copyFrom(op.child1);
        child2.copyFrom(op.child2);
        child3.copyFrom(op.child3);
        this.opKind = op.opKind;
        super.copyFrom(op);
        return true;
    }

    @Override
    public boolean sameAs(ExprTree tree) {
        boolean is_same = false;
        Operation other_op;
        ExprTree pSelfTree = tree.self();

        if (pSelfTree.getKind() != NodeKind.OP_NODE) {
            is_same = false;
        } else {
            other_op = (Operation) pSelfTree;
            if (opKind == other_op.opKind && sameChild(child1, other_op.child1) && sameChild(child2, other_op.child2)
                    && sameChild(child3, other_op.child3)) {
                is_same = true;
            } else {
                is_same = false;
            }
        }
        return is_same;
    }

    public boolean sameChild(ExprTree tree1, ExprTree tree2) {
        boolean is_same = false;
        if (tree1 == null) {
            if (tree2 == null) {
                is_same = true;
            } else {
                is_same = false;
            }
        } else if (tree2 == null) {
            is_same = false;
        } else {
            is_same = tree1.sameAs(tree2);
        }
        return is_same;
    }

    @Override
    public void privateSetParentScope(ClassAd parent) {
        child1.setParentScope(parent);
        child2.setParentScope(parent);
        child3.setParentScope(parent);
    }

    public static void operate(int opKind, Value op1, Value op2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        Value dummy = objectPool.valuePool.get();
        privateDoOperation(opKind, op1, op2, dummy, true, true, false, result, null, objectPool);
    }

    public void operate(int op, Value op1, Value op2, Value op3, Value result) throws HyracksDataException {
        privateDoOperation(op, op1, op2, op3, true, true, true, result, null, objectPool);
    }

    public static int privateDoOperation(int op, Value val1, Value val2, Value val3, boolean valid1, boolean valid2,
            boolean valid3, Value result, ClassAdObjectPool objectPool) throws HyracksDataException {
        return privateDoOperation(op, val1, val2, val3, valid1, valid2, valid3, result, null, objectPool);
    }

    public static int privateDoOperation(int op, Value val1, Value val2, Value val3, boolean valid1, boolean valid2,
            boolean valid3, Value result, EvalState es, ClassAdObjectPool objectPool) throws HyracksDataException {
        ValueType vt1;
        ValueType vt2;
        ValueType vt3;

        // get the types of the values
        vt1 = val1.getType();
        vt2 = val2.getType();
        vt3 = val3.getType();

        // take care of the easy cases
        if (op == OpKind_NO_OP || op == OpKind_PARENTHESES_OP) {
            result.setValue(val1);
            return SigValues.SIG_CHLD1.ordinal();
        } else if (op == OpKind_UNARY_PLUS_OP) {
            if (vt1 == ValueType.BOOLEAN_VALUE || vt1 == ValueType.STRING_VALUE || val1.isListValue()
                    || vt1 == ValueType.CLASSAD_VALUE || vt1 == ValueType.ABSOLUTE_TIME_VALUE) {
                result.setErrorValue();
            } else {
                // applies for ERROR, UNDEFINED and Numbers
                result.setValue(val1);
            }
            return SigValues.SIG_CHLD1.ordinal();
        }

        // test for cases when evaluation is strict
        if (isStrictOperator(op)) {
            // check for error values
            if (vt1 == ValueType.ERROR_VALUE) {
                result.setErrorValue();
                return SigValues.SIG_CHLD1.ordinal();
            }
            if (valid2 && vt2 == ValueType.ERROR_VALUE) {
                result.setErrorValue();
                return SigValues.SIG_CHLD2.ordinal();
            }
            if (valid3 && vt3 == ValueType.ERROR_VALUE) {
                result.setErrorValue();
                return SigValues.SIG_CHLD3.ordinal();
            }

            // check for undefined values.  we need to check if the corresponding
            // tree exists, because these values would be undefined" anyway then.
            if (valid1 && vt1 == ValueType.UNDEFINED_VALUE) {
                result.setUndefinedValue();
                return SigValues.SIG_CHLD1.ordinal();
            }
            if (valid2 && vt2 == ValueType.UNDEFINED_VALUE) {
                result.setUndefinedValue();
                return SigValues.SIG_CHLD2.ordinal();
            }
            if (valid3 && vt3 == ValueType.UNDEFINED_VALUE) {
                result.setUndefinedValue();
                return SigValues.SIG_CHLD3.ordinal();
            }
        }

        // comparison operations (binary, one unary)
        if (op >= OpKind_COMPARISON_START && op <= OpKind_COMPARISON_END) {
            return (doComparison(op, val1, val2, result, objectPool));
        }

        // arithmetic operations (binary)
        if (op >= OpKind_ARITHMETIC_START && op <= OpKind_ARITHMETIC_END) {
            return (doArithmetic(op, val1, val2, result, objectPool));
        }

        // logical operators (binary, one unary)
        if (op >= OpKind_LOGIC_START && op <= OpKind_LOGIC_END) {
            return (doLogical(op, val1, val2, result, objectPool));
        }

        // bitwise operators (binary, one unary)
        if (op >= OpKind_BITWISE_START && op <= OpKind_BITWISE_END) {
            return (doBitwise(op, val1, val2, result, objectPool));
        }

        // misc.
        if (op == OpKind_TERNARY_OP) {
            // ternary (if-operator)
            MutableBoolean b = objectPool.boolPool.get();

            // if the selector is UNDEFINED, the result is undefined
            if (vt1 == ValueType.UNDEFINED_VALUE) {
                result.setUndefinedValue();
                return SigValues.SIG_CHLD1.ordinal();
            }

            if (!val1.isBooleanValueEquiv(b)) {
                result.setErrorValue();
                return SigValues.SIG_CHLD1.ordinal();
            } else if (b.booleanValue()) {
                result.setValue(val2);
                return (SigValues.SIG_CHLD2.ordinal());
            } else {
                result.setValue(val3);
                return (SigValues.SIG_CHLD3.ordinal());
            }
        } else if (op == OpKind_SUBSCRIPT_OP) {
            // subscripting from a list (strict)

            if (vt1 == ValueType.CLASSAD_VALUE && vt2 == ValueType.STRING_VALUE) {
                ClassAd classad = objectPool.classAdPool.get();
                AMutableCharArrayString index = objectPool.strPool.get();

                val1.isClassAdValue(classad);
                val2.isStringValue(index);

                if (classad.lookup(index.toString()) == null) {
                    result.setErrorValue();
                    return SigValues.SIG_CHLD2.ordinal();
                }
                if (!classad.evaluateAttr(index.toString(), result)) {
                    result.setErrorValue();
                    return SigValues.SIG_CHLD2.ordinal();
                }

                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            } else if (val1.isListValue() && vt2 == ValueType.INTEGER_VALUE) {
                AMutableInt64 index = objectPool.int64Pool.get();
                ExprList elist = objectPool.exprListPool.get();

                val1.isListValue(elist);
                val2.isIntegerValue(index);

                // check bounds
                if (index.getLongValue() < 0 || index.getLongValue() >= elist.getExprList().size()) {
                    result.setErrorValue();
                    return SigValues.SIG_CHLD2.ordinal();
                }
                // get value
                elist.getValue(result, elist.get((int) index.getLongValue()), es);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }
            // should not reach here
            throw new HyracksDataException("Should not get here");
        }
        return -1;
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value result) throws HyracksDataException {
        Value val1 = objectPool.valuePool.get();
        Value val2 = objectPool.valuePool.get();
        Value val3 = objectPool.valuePool.get();
        boolean valid1, valid2, valid3;
        int rval = 0;

        valid1 = false;
        valid2 = false;
        valid3 = false;

        AMutableInt32 operationKind = objectPool.int32Pool.get();
        ExprTreeHolder child1 = objectPool.mutableExprPool.get();
        ExprTreeHolder child2 = objectPool.mutableExprPool.get();
        ExprTreeHolder child3 = objectPool.mutableExprPool.get();
        getComponents(operationKind, child1, child2, child3);

        // Evaluate all valid children
        if (child1.getInnerTree() != null) {
            if (!child1.publicEvaluate(state, val1)) {
                result.setErrorValue();
                return (false);
            }
            valid1 = true;

            if (shortCircuit(state, val1, result)) {
                return true;
            }
        }

        if (child2.getInnerTree() != null) {
            if (!child2.publicEvaluate(state, val2)) {
                result.setErrorValue();
                return (false);
            }
            valid2 = true;
        }
        if (child3.getInnerTree() != null) {
            if (!child3.publicEvaluate(state, val3)) {
                result.setErrorValue();
                return (false);
            }
            valid3 = true;
        }

        rval = privateDoOperation(opKind, val1, val2, val3, valid1, valid2, valid3, result, state, objectPool);

        return (rval != SigValues.SIG_NONE.ordinal());
    }

    public boolean shortCircuit(EvalState state, Value arg1, Value result) throws HyracksDataException {
        MutableBoolean arg1_bool = objectPool.boolPool.get();
        switch (opKind) {
            case OpKind_LOGICAL_OR_OP:
                if (arg1.isBooleanValueEquiv(arg1_bool) && arg1_bool.booleanValue()) {
                    result.setBooleanValue(true);
                    return true;
                }
                break;
            case OpKind_LOGICAL_AND_OP:
                if (arg1.isBooleanValueEquiv(arg1_bool) && !arg1_bool.booleanValue()) {
                    result.setBooleanValue(false);
                    return true;
                }
                break;
            case OpKind_TERNARY_OP:
                if (arg1.isBooleanValueEquiv(arg1_bool)) {
                    if (arg1_bool.booleanValue()) {
                        if (child2 != null) {
                            return child2.publicEvaluate(state, result);
                        }
                    } else {
                        if (child3 != null) {
                            return child3.publicEvaluate(state, result);
                        }
                    }
                }
                break;
            default:
                // no-op
                break;
        }
        return false;
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value result, ExprTreeHolder tree) throws HyracksDataException {
        int sig;
        Value val1 = objectPool.valuePool.get();
        Value val2 = objectPool.valuePool.get();
        Value val3 = objectPool.valuePool.get();
        ExprTreeHolder t1 = objectPool.mutableExprPool.get();
        ExprTreeHolder t2 = objectPool.mutableExprPool.get();
        ExprTreeHolder t3 = objectPool.mutableExprPool.get();
        boolean valid1 = false, valid2 = false, valid3 = false;
        AMutableInt32 opKind = objectPool.int32Pool.get();
        ExprTreeHolder child1 = objectPool.mutableExprPool.get();
        ExprTreeHolder child2 = objectPool.mutableExprPool.get();
        ExprTreeHolder child3 = objectPool.mutableExprPool.get();
        getComponents(opKind, child1, child2, child3);

        // Evaluate all valid children
        tree = objectPool.mutableExprPool.get();
        if (child1.getInnerTree() != null) {
            if (!child1.publicEvaluate(state, val1, t1)) {
                result.setErrorValue();
                return (false);
            }
            valid1 = true;
        }

        if (child2.getInnerTree() != null) {
            if (!child2.publicEvaluate(state, val2, t2)) {
                result.setErrorValue();
                return (false);
            }
            valid2 = true;
        }
        if (child3.getInnerTree() != null) {
            if (!child3.publicEvaluate(state, val3, t3)) {
                result.setErrorValue();
                return (false);
            }
            valid3 = true;
        }

        // do evaluation
        sig = privateDoOperation(opKind.getIntegerValue(), val1, val2, val3, valid1, valid2, valid3, result, state,
                objectPool);

        // delete trees which were not significant
        if (valid1 && 0 != (sig & SigValues.SIG_CHLD1.ordinal())) {
            t1 = null;
        }
        if (valid2 && 0 != (sig & SigValues.SIG_CHLD2.ordinal())) {
            t2 = null;
        }
        if (valid3 && 0 != (sig & SigValues.SIG_CHLD3.ordinal())) {
            t3 = null;
        }

        if (sig == SigValues.SIG_NONE.ordinal()) {
            result.setErrorValue();
            tree.setInnerTree(null);
            return (false);
        }

        // in case of strict operators, if a subexpression is significant and the
        // corresponding value is UNDEFINED or ERROR, propagate only that tree
        if (isStrictOperator(opKind.getIntegerValue())) {
            // strict unary operators:  unary -, unary +, !, ~, ()
            if (opKind.getIntegerValue() == OpKind_UNARY_MINUS_OP || opKind.getIntegerValue() == OpKind_UNARY_PLUS_OP
                    || opKind.getIntegerValue() == OpKind_LOGICAL_NOT_OP
                    || opKind.getIntegerValue() == OpKind_BITWISE_NOT_OP
                    || opKind.getIntegerValue() == OpKind_PARENTHESES_OP) {
                if (val1.isExceptional()) {
                    // the operator is only propagating the value;  only the
                    // subexpression is significant
                    tree.setInnerTree(t1);
                } else {
                    // the node operated on the value; the operator is also
                    // significant
                    tree.setInnerTree(createOperation(opKind.getIntegerValue(), t1, objectPool));
                }
                return (true);
            } else {
                // strict binary operators
                if (val1.isExceptional() || val2.isExceptional()) {
                    // exceptional values are only being propagated
                    if (0 != (SigValues.values()[sig].ordinal() & SigValues.SIG_CHLD1.ordinal())) {
                        tree.setInnerTree(t1);
                        return (true);
                    } else if (0 != (SigValues.values()[sig].ordinal() & SigValues.SIG_CHLD2.ordinal())) {
                        tree.setInnerTree(t2);
                        return (true);
                    }
                    throw new HyracksDataException("Should not reach here");
                } else {
                    // the node is also significant
                    tree.setInnerTree(createOperation(opKind.getIntegerValue(), t1, t2, objectPool));
                    return (true);
                }
            }
        } else {
            // non-strict operators
            if (opKind.getIntegerValue() == OpKind_IS_OP || opKind.getIntegerValue() == OpKind_ISNT_OP) {
                // the operation is *always* significant for IS and ISNT
                tree.setInnerTree(createOperation(opKind.getIntegerValue(), t1, t2, objectPool));
                return (true);
            }
            // other non-strict binary operators
            if (opKind.getIntegerValue() == OpKind_LOGICAL_AND_OP || opKind.getIntegerValue() == OpKind_LOGICAL_OR_OP) {
                if ((SigValues.values()[sig].ordinal() & SigValues.SIG_CHLD1.ordinal()) != 0
                        && (SigValues.values()[sig].ordinal() & SigValues.SIG_CHLD2.ordinal()) != 0) {
                    tree.setInnerTree(createOperation(opKind.getIntegerValue(), t1, t2, objectPool));
                    return (true);
                } else if ((SigValues.values()[sig].ordinal() & SigValues.SIG_CHLD1.ordinal()) != 0) {
                    tree.setInnerTree(t1);
                    return (true);
                } else if ((SigValues.values()[sig].ordinal() & SigValues.SIG_CHLD2.ordinal()) != 0) {
                    tree.setInnerTree(t2);
                    return (true);
                } else {
                    throw new HyracksDataException("Shouldn't reach here");
                }
            }
            // non-strict ternary operator (conditional operator) s ? t : f
            // selector is always significant (???)
            if (opKind.getIntegerValue() == OpKind_TERNARY_OP) {
                Value tmpVal = objectPool.valuePool.get();
                tmpVal.setUndefinedValue();
                tree.setInnerTree(Literal.createLiteral(tmpVal, objectPool));

                // "true" consequent taken
                if ((SigValues.values()[sig].ordinal() & SigValues.SIG_CHLD2.ordinal()) != 0) {
                    tree.setInnerTree(t2);
                    return (true);
                } else if ((SigValues.values()[sig].ordinal() & SigValues.SIG_CHLD3.ordinal()) != 0) {
                    tree.setInnerTree(t3);
                    return (true);
                }
                // neither consequent; selector was exceptional; return ( s )
                tree.setInnerTree(t1);
                return (true);
            }
        }
        throw new HyracksDataException("Should not reach here");
    }

    @Override
    public boolean privateFlatten(EvalState state, Value val, ExprTreeHolder tree, AMutableInt32 opPtr)
            throws HyracksDataException {
        AMutableInt32 childOp1 = objectPool.int32Pool.get();
        childOp1.setValue(OpKind_NO_OP);
        AMutableInt32 childOp2 = objectPool.int32Pool.get();
        childOp2.setValue(OpKind_NO_OP);
        ExprTreeHolder fChild1 = objectPool.mutableExprPool.get();
        ExprTreeHolder fChild2 = objectPool.mutableExprPool.get();
        Value val1 = objectPool.valuePool.get();
        Value val2 = objectPool.valuePool.get();
        Value val3 = objectPool.valuePool.get();
        AMutableInt32 newOp = objectPool.int32Pool.get();
        newOp.setValue(opKind);
        int op = opKind;

        tree.setInnerTree(null);// Just to be safe...  wenger 2003-12-11.

        // if op is binary, but not associative or commutative, disallow splitting
        if ((op >= OpKind_COMPARISON_START && op <= OpKind_COMPARISON_END) || op == OpKind_SUBTRACTION_OP
                || op == OpKind_DIVISION_OP || op == OpKind_MODULUS_OP || op == OpKind_LEFT_SHIFT_OP
                || op == OpKind_RIGHT_SHIFT_OP || op == OpKind_URIGHT_SHIFT_OP) {
            if (opPtr != null) {
                opPtr.setValue(OpKind_NO_OP);
            }
            if (child1.publicFlatten(state, val1, fChild1) && child2.publicFlatten(state, val2, fChild2)) {
                if (fChild1.getInnerTree() == null && fChild2.getInnerTree() == null) {
                    privateDoOperation(op, val1, val2, val3, true, true, false, val, objectPool);
                    tree.setInnerTree(null);
                    return true;
                } else if (fChild1.getInnerTree() != null && fChild2.getInnerTree() != null) {
                    tree.setInnerTree(Operation.createOperation(op, fChild1, fChild2, objectPool));
                    return true;
                } else if (fChild1.getInnerTree() != null) {
                    tree.setInnerTree(Operation.createOperation(op, fChild1, val2, objectPool));
                    return true;
                } else if (fChild2.getInnerTree() != null) {
                    tree.setInnerTree(Operation.createOperation(op, val1, fChild2, objectPool));
                    return true;
                }
            } else {
                tree.setInnerTree(null);
                return false;
            }
        } else
        // now catch all non-binary operators
        if (op == OpKind_TERNARY_OP || op == OpKind_SUBSCRIPT_OP || op == OpKind_UNARY_PLUS_OP
                || op == OpKind_UNARY_MINUS_OP || op == OpKind_PARENTHESES_OP || op == OpKind_LOGICAL_NOT_OP
                || op == OpKind_BITWISE_NOT_OP) {
            return flattenSpecials(state, val, tree);
        }

        // any op that got past the above is binary, commutative and associative
        // Flatten sub expressions
        if ((child1.getInnerTree() != null && !child1.publicFlatten(state, val1, fChild1, childOp1))
                || (child2.getInnerTree() != null && !child2.publicFlatten(state, val2, fChild2, childOp2))) {
            tree.setInnerTree(null);
            return false;
        }

        // NOTE: combine() deletes fChild1 and/or fChild2 if they are not
        // included in tree
        if (!combine(newOp, val, tree, childOp1, val1, fChild1, childOp2, val2, fChild2)) {
            tree.setInnerTree(null);
            if (opPtr != null) {
                opPtr.setValue(OpKind_NO_OP);
            }
            return false;
        }

        // if splitting is disallowed, fold the value and tree into a tree
        if (opPtr == null && newOp.getIntegerValue() != OpKind_NO_OP) {
            tree.setInnerTree(Operation.createOperation(newOp.getIntegerValue(), val, tree, objectPool));
            if (tree.getInnerTree() == null) {
                return false;
            }
            return true;
        } else if (opPtr != null) {
            opPtr.setValue(newOp.getIntegerValue());
        }
        return true;
    }

    public boolean combine(AMutableInt32 op, Value val, ExprTreeHolder tree, AMutableInt32 op1, Value val1,
            ExprTreeHolder tree1, AMutableInt32 op2, Value val2, ExprTreeHolder tree2) throws HyracksDataException {
        Operation newOp = objectPool.operationPool.get();
        Value dummy = objectPool.valuePool.get(); // undefined

        // special don't care cases for logical operators with exactly one value
        if ((tree1.getInnerTree() == null || tree2.getInnerTree() == null)
                && (tree1.getInnerTree() != null || tree2.getInnerTree() != null)
                && (op.getIntegerValue() == OpKind_LOGICAL_OR_OP || op.getIntegerValue() == OpKind_LOGICAL_AND_OP)) {
            privateDoOperation(op.getIntegerValue(), tree1.getInnerTree() == null ? val1 : dummy,
                    tree2.getInnerTree() == null ? val2 : dummy, dummy, true, true, false, val, objectPool);
            if (val.isBooleanValue()) {
                tree.setInnerTree(null);
                op.setValue(OpKind_NO_OP);
                return true;
            }
        }

        if (tree1.getInnerTree() == null && tree2.getInnerTree() == null) {
            // left and rightsons are only values
            privateDoOperation(op.getIntegerValue(), val1, val2, dummy, true, true, false, val, objectPool);
            tree.setInnerTree(null);
            op.setValue(OpKind_NO_OP);
            return true;
        } else if (tree1.getInnerTree() == null
                && (tree2.getInnerTree() != null && op2.getIntegerValue() == OpKind_NO_OP)) {
            // leftson is a value, rightson is a tree
            tree.setInnerTree(tree2.getInnerTree());
            val.setValue(val1);
            return true;
        } else if (tree2.getInnerTree() == null
                && (tree1.getInnerTree() != null && op1.getIntegerValue() == OpKind_NO_OP)) {
            // rightson is a value, leftson is a tree
            tree.setInnerTree(tree1.getInnerTree());
            val.setValue(val2);
            return true;
        } else if ((tree1.getInnerTree() != null && op1.getIntegerValue() == OpKind_NO_OP)
                && (tree2.getInnerTree() != null && op2.getIntegerValue() == OpKind_NO_OP)) {
            // left and rightsons are trees only
            if (null != (newOp = createOperation(op.getIntegerValue(), tree1, tree2, objectPool))) {
                return false;
            }
            tree.setInnerTree(newOp);
            op.setValue(OpKind_NO_OP);
            return true;
        }

        // cannot collapse values due to dissimilar ops
        if ((op1.getIntegerValue() != OpKind_NO_OP || op2.getIntegerValue() != OpKind_NO_OP) && !op.equals(op1)
                && !op.equals(op1)) {
            // at least one of them returned a value and a tree, and parent does
            // not share the same operation with either child
            ExprTreeHolder newOp1 = objectPool.mutableExprPool.get();
            ExprTreeHolder newOp2 = objectPool.mutableExprPool.get();

            if (op1.getIntegerValue() != OpKind_NO_OP) {
                newOp1.setInnerTree(Operation.createOperation(op1.getIntegerValue(), val1, tree1, objectPool));
            } else if (tree1.getInnerTree() != null) {
                newOp1.setInnerTree(tree1.getInnerTree());
            } else {
                newOp1.setInnerTree(Literal.createLiteral(val1, objectPool));
            }

            if (op2.getIntegerValue() != OpKind_NO_OP) {
                newOp2.setInnerTree(Operation.createOperation(op2.getIntegerValue(), val2, tree2, objectPool));
            } else if (tree2.getInnerTree() != null) {
                newOp2.setInnerTree(tree2);
            } else {
                newOp2.setInnerTree(Literal.createLiteral(val2, objectPool));
            }

            if (newOp1.getInnerTree() == null || newOp2.getInnerTree() == null) {
                tree.setInnerTree(null);
                op.setValue(OpKind_NO_OP);
                return false;
            }
            newOp = createOperation(op.getIntegerValue(), newOp1, newOp2, objectPool);
            if (newOp == null) {
                tree.setInnerTree(null);
                op.setValue(OpKind_NO_OP);
                return false;
            }
            op.setValue(OpKind_NO_OP);
            tree.setInnerTree(newOp);
            return true;
        }

        if (op.equals(op1) && op.equals(op2)) {
            // same operators on both children . since op!=NO_OP, neither are op1,
            // op2.  so they both make tree and value contributions
            newOp = createOperation(op.getIntegerValue(), tree1, tree2, objectPool);
            if (newOp == null) {
                return false;
            }
            privateDoOperation(op.getIntegerValue(), val1, val2, dummy, true, true, false, val, objectPool);
            tree.setInnerTree(newOp);
            return true;
        } else if (op.equals(op1)) {
            // leftson makes a tree,value contribution
            if (tree2.getInnerTree() == null) {
                // rightson makes a value contribution
                privateDoOperation(op.getIntegerValue(), val1, val2, dummy, true, true, false, val, objectPool);
                tree.setInnerTree(tree1);
                return true;
            } else {
                // rightson makes a tree contribution
                Operation local_newOp = createOperation(op.getIntegerValue(), tree1, tree2, objectPool);
                if (local_newOp == null) {
                    tree.setInnerTree(null);
                    op.setValue(OpKind_NO_OP);
                    return false;
                }
                val.setValue(val1);
                tree.setInnerTree(local_newOp); // NAC - BUG FIX
                return true;
            }
        } else if (op.equals(op2)) {
            // rightson makes a tree,value contribution
            if (tree1.getInnerTree() == null) {
                // leftson makes a value contribution
                privateDoOperation(op.getIntegerValue(), val1, val2, dummy, true, true, false, val, objectPool);
                tree.setInnerTree(tree2);
                return true;
            } else {
                // leftson makes a tree contribution
                Operation local_newOp = createOperation(op.getIntegerValue(), tree1, tree2, objectPool);
                if (local_newOp == null) {
                    tree.setInnerTree(null);
                    op.setValue(OpKind_NO_OP);
                    return false;
                }
                tree.setInnerTree(local_newOp); // NAC BUG FIX
                val.setValue(val2);
                return true;
            }
        }

        throw new HyracksDataException("Should not reach here");
    }

    public static int doComparison(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        ValueType vt1;
        ValueType vt2;
        ValueType coerceResult;

        if (op == OpKind_META_EQUAL_OP || op == OpKind_META_NOT_EQUAL_OP) {
            // do not do type promotions for the meta operators
            vt1 = v1.getType();
            vt2 = v2.getType();
            coerceResult = vt1;
        } else {
            // do numerical type promotions --- other types/values are unchanged
            coerceResult = coerceToNumber(v1, v2, objectPool);
            vt1 = v1.getType();
            vt2 = v2.getType();
        }

        // perform comparison for =?= ; true iff same types and same values
        if (op == OpKind_META_EQUAL_OP) {
            if (vt1 != vt2) {
                result.setBooleanValue(false);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            // undefined or error
            if (vt1 == ValueType.UNDEFINED_VALUE || vt1 == ValueType.ERROR_VALUE) {
                result.setBooleanValue(true);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }
        }
        // perform comparison for =!= ; negation of =?=
        if (op == OpKind_META_NOT_EQUAL_OP) {
            if (vt1 != vt2) {
                result.setBooleanValue(true);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            // undefined or error
            if (vt1 == ValueType.UNDEFINED_VALUE || vt1 == ValueType.ERROR_VALUE) {
                result.setBooleanValue(false);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }
        }

        switch (coerceResult) {
            // at least one of v1, v2 is a string
            case STRING_VALUE:
                // check if both are strings
                if (vt1 != ValueType.STRING_VALUE || vt2 != ValueType.STRING_VALUE) {
                    // comparison between strings and non-exceptional non-string
                    // values is error
                    result.setErrorValue();
                    return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
                }
                compareStrings(op, v1, v2, result, objectPool);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

            case INTEGER_VALUE:
                compareIntegers(op, v1, v2, result, objectPool);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

            case REAL_VALUE:
                compareReals(op, v1, v2, result, objectPool);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

            case BOOLEAN_VALUE:
                // check if both are bools
                if (!v1.isBooleanValue() || !v2.isBooleanValue()) {
                    result.setErrorValue();
                    return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
                }
                compareBools(op, v1, v2, result, objectPool);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

            case LIST_VALUE:
            case SLIST_VALUE:
            case CLASSAD_VALUE:
                result.setErrorValue();
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

            case ABSOLUTE_TIME_VALUE:
                if (!v1.isAbsoluteTimeValue() || !v2.isAbsoluteTimeValue()) {
                    result.setErrorValue();
                    return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
                }
                compareAbsoluteTimes(op, v1, v2, result, objectPool);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

            case RELATIVE_TIME_VALUE:
                if (!v1.isRelativeTimeValue() || !v2.isRelativeTimeValue()) {
                    result.setErrorValue();
                    return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
                }
                compareRelativeTimes(op, v1, v2, result, objectPool);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

            default:
                // should not get here
                throw new HyracksDataException("Should not get here");
        }
    }

    public static int doArithmetic(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        AMutableInt64 i1 = objectPool.int64Pool.get();
        AMutableInt64 i2 = objectPool.int64Pool.get();
        ClassAdTime t1 = objectPool.classAdTimePool.get();
        AMutableDouble r1 = objectPool.doublePool.get();
        MutableBoolean b1 = objectPool.boolPool.get();

        // ensure the operands have arithmetic types
        if ((!v1.isIntegerValue() && !v1.isRealValue() && !v1.isAbsoluteTimeValue() && !v1.isRelativeTimeValue()
                && !v1.isBooleanValue())
                || (op != OpKind_UNARY_MINUS_OP && !v2.isBooleanValue() && !v2.isIntegerValue() && !v2.isRealValue()
                        && !v2.isAbsoluteTimeValue() && !v2.isRelativeTimeValue())) {
            result.setErrorValue();
            return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
        }

        // take care of the unary arithmetic operators
        if (op == OpKind_UNARY_MINUS_OP) {
            if (v1.isIntegerValue(i1)) {
                result.setIntegerValue((-1L) * i1.getLongValue());
                return SigValues.SIG_CHLD1.ordinal();
            } else if (v1.isRealValue(r1)) {
                result.setRealValue((-1) * r1.getDoubleValue());
                return SigValues.SIG_CHLD1.ordinal();
            } else if (v1.isRelativeTimeValue(t1)) {
                t1.setValue((-1) * t1.getTimeInMillis());
                result.setRelativeTimeValue(t1);
                return (SigValues.SIG_CHLD1.ordinal());
            } else if (v1.isBooleanValue(b1)) {
                result.setBooleanValue(!b1.booleanValue());
            } else if (v1.isExceptional()) {
                // undefined or error --- same as operand
                result.setValue(v1);
                return SigValues.SIG_CHLD1.ordinal();
            }
            // unary minus not defined on any other operand type
            result.setErrorValue();
            return (SigValues.SIG_CHLD1.ordinal());
        }

        // perform type promotions and proceed with arithmetic
        switch (coerceToNumber(v1, v2, objectPool)) {
            case INTEGER_VALUE:
                v1.isIntegerValue(i1);
                v2.isIntegerValue(i2);
                switch (op) {
                    case OpKind_ADDITION_OP:
                        result.setIntegerValue(i1.getLongValue() + i2.getLongValue());
                        return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

                    case OpKind_SUBTRACTION_OP:
                        result.setIntegerValue(i1.getLongValue() - i2.getLongValue());
                        return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

                    case OpKind_MULTIPLICATION_OP:
                        result.setIntegerValue(i1.getLongValue() * i2.getLongValue());
                        return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

                    case OpKind_DIVISION_OP:
                        if (i2.getLongValue() != 0L) {
                            result.setIntegerValue(i1.getLongValue() / i2.getLongValue());
                        } else {
                            result.setErrorValue();
                        }
                        return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

                    case OpKind_MODULUS_OP:
                        if (i2.getLongValue() != 0) {
                            result.setIntegerValue(i1.getLongValue() % i2.getLongValue());
                        } else {
                            result.setErrorValue();
                        }
                        return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());

                    default:
                        // should not reach here
                        throw new HyracksDataException("Should not get here");
                }

            case REAL_VALUE: {
                return (doRealArithmetic(op, v1, v2, result, objectPool));
            }
            case ABSOLUTE_TIME_VALUE:
            case RELATIVE_TIME_VALUE: {
                return (doTimeArithmetic(op, v1, v2, result, objectPool));
            }
            default:
                // should not get here
                throw new HyracksDataException("Should not get here");
        }
    }

    public static int doLogical(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        MutableBoolean b1 = objectPool.boolPool.get();
        MutableBoolean b2 = objectPool.boolPool.get();

        // first coerece inputs to boolean if they are considered equivalent
        if (!v1.isBooleanValue(b1) && v1.isBooleanValueEquiv(b1)) {
            v1.setBooleanValue(b1.booleanValue());
        }
        if (!v2.isBooleanValue(b2) && v2.isBooleanValueEquiv(b2)) {
            v2.setBooleanValue(b2);
        }

        ValueType vt1 = v1.getType();
        ValueType vt2 = v2.getType();

        if (vt1 != ValueType.UNDEFINED_VALUE && vt1 != ValueType.ERROR_VALUE && vt1 != ValueType.BOOLEAN_VALUE) {
            result.setErrorValue();
            return SigValues.SIG_CHLD1.ordinal();
        }
        if (vt2 != ValueType.UNDEFINED_VALUE && vt2 != ValueType.ERROR_VALUE && vt2 != ValueType.BOOLEAN_VALUE) {
            result.setErrorValue();
            return SigValues.SIG_CHLD2.ordinal();
        }

        // handle unary operator
        if (op == OpKind_LOGICAL_NOT_OP) {
            if (vt1 == ValueType.BOOLEAN_VALUE) {
                result.setBooleanValue(!b1.booleanValue());
            } else {
                result.setValue(v1);
            }
            return SigValues.SIG_CHLD1.ordinal();
        }

        if (op == OpKind_LOGICAL_OR_OP) {
            if (vt1 == ValueType.BOOLEAN_VALUE && b1.booleanValue()) {
                result.setBooleanValue(true);
                return SigValues.SIG_CHLD1.ordinal();
            } else if (vt1 == ValueType.ERROR_VALUE) {
                result.setErrorValue();
                return SigValues.SIG_CHLD1.ordinal();
            } else if (vt1 == ValueType.BOOLEAN_VALUE && !b1.booleanValue()) {
                result.setValue(v2);
            } else if (vt2 != ValueType.BOOLEAN_VALUE) {
                result.setValue(v2);
            } else if (b2.booleanValue()) {
                result.setBooleanValue(true);
            } else {
                result.setUndefinedValue();
            }
            return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
        } else if (op == OpKind_LOGICAL_AND_OP) {
            if (vt1 == ValueType.BOOLEAN_VALUE && !b1.booleanValue()) {
                result.setBooleanValue(false);
                return SigValues.SIG_CHLD1.ordinal();
            } else if (vt1 == ValueType.ERROR_VALUE) {
                result.setErrorValue();
                return SigValues.SIG_CHLD1.ordinal();
            } else if (vt1 == ValueType.BOOLEAN_VALUE && b1.booleanValue()) {
                result.setValue(v2);
            } else if (vt2 != ValueType.BOOLEAN_VALUE) {
                result.setValue(v2);
            } else if (!b2.booleanValue()) {
                result.setBooleanValue(false);
            } else {
                result.setUndefinedValue();
            }
            return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
        }

        throw new HyracksDataException("Shouldn't reach here");
    }

    public static int doBitwise(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        AMutableInt64 i1 = objectPool.int64Pool.get();
        AMutableInt64 i2 = objectPool.int64Pool.get();

        // bitwise operations are defined only on integers
        if (op == OpKind_BITWISE_NOT_OP) {
            if (!v1.isIntegerValue(i1)) {
                result.setErrorValue();
                return SigValues.SIG_CHLD1.ordinal();
            }
        } else if (!v1.isIntegerValue(i1) || !v2.isIntegerValue(i2)) {
            result.setErrorValue();
            return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
        }

        switch (op) {
            case OpKind_BITWISE_NOT_OP:
                result.setIntegerValue(~(i1.getLongValue()));
                break;
            case OpKind_BITWISE_OR_OP:
                result.setIntegerValue(i1.getLongValue() | i2.getLongValue());
                break;
            case OpKind_BITWISE_AND_OP:
                result.setIntegerValue(i1.getLongValue() & i2.getLongValue());
                break;
            case OpKind_BITWISE_XOR_OP:
                result.setIntegerValue(i1.getLongValue() ^ i2.getLongValue());
                break;
            case OpKind_LEFT_SHIFT_OP:
                result.setIntegerValue(i1.getLongValue() << i2.getLongValue());
                break;

            case OpKind_URIGHT_SHIFT_OP:
                //               if (i1 >= 0) {
                // Could probably just use >>>
                // sign bit is not on;  >> will work fine
                result.setIntegerValue(i1.getLongValue() >>> i2.getLongValue());
                break;
            //               } else {
            // sign bit is on
            //                  val.setValue(i1 >> 1);      // shift right 1; the sign bit *may* be on
            //                  val.setValue(val.getLongValue() & (~signMask)); // Clear the sign bit for sure
            //                  val.setValue(val.getLongValue() >>(i2.getLongValue() - 1));   // shift remaining Number of positions
            //                  result.SetIntegerValue (val.getLongValue());
            //                  break;
            //               }

            case OpKind_RIGHT_SHIFT_OP:
                // sign bit is off;  >> will work fine
                result.setIntegerValue(i1.getLongValue() >> i2.getLongValue());
                break;

            default:
                // should not get here
                throw new HyracksDataException("Should not get here");
        }

        if (op == OpKind_BITWISE_NOT_OP) {
            return SigValues.SIG_CHLD1.ordinal();
        }

        return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
    }

    //out of domain value
    public static final int EDOM = 33;

    public static int doRealArithmetic(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        AMutableDouble r1 = objectPool.doublePool.get();
        AMutableDouble r2 = objectPool.doublePool.get();
        double comp = 0;

        // we want to prevent FPE and set the ERROR value on the result; on Unix
        // trap sigfpe and set the ClassAdExprFPE flag to true; on NT check the
        // result against HUGE_VAL.  check errno for EDOM and ERANGE for kicks.

        v1.isRealValue(r1);
        v2.isRealValue(r2);
        int errno = 0;
        switch (op) {
            case OpKind_ADDITION_OP:
                comp = r1.getDoubleValue() + r2.getDoubleValue();
                break;
            case OpKind_SUBTRACTION_OP:
                comp = r1.getDoubleValue() - r2.getDoubleValue();
                break;
            case OpKind_MULTIPLICATION_OP:
                comp = r1.getDoubleValue() * r2.getDoubleValue();
                break;
            case OpKind_DIVISION_OP:
                comp = r1.getDoubleValue() / r2.getDoubleValue();
                break;
            case OpKind_MODULUS_OP:
                errno = EDOM;
                break;
            default:
                // should not reach here
                throw new HyracksDataException("Should not get here");
        }

        // check if anything bad happened
        if (errno == EDOM) {
            result.setErrorValue();
        } else {
            result.setRealValue(comp);
        }
        return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
    }

    public static int doTimeArithmetic(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool) {
        ClassAdTime asecs1 = objectPool.classAdTimePool.get();
        ClassAdTime asecs2 = objectPool.classAdTimePool.get();
        ValueType vt1 = v1.getType();
        ValueType vt2 = v2.getType();

        // addition
        if (op == OpKind_ADDITION_OP) {
            if (vt1 == ValueType.ABSOLUTE_TIME_VALUE && vt2 == ValueType.RELATIVE_TIME_VALUE) {
                v1.isAbsoluteTimeValue(asecs1);
                v2.isRelativeTimeValue(asecs2);
                asecs1.setValue(asecs1.getTimeInMillis() + asecs2.getTimeInMillis());
                result.setAbsoluteTimeValue(asecs1);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            if (vt1 == ValueType.RELATIVE_TIME_VALUE && vt2 == ValueType.ABSOLUTE_TIME_VALUE) {
                v1.isRelativeTimeValue(asecs1);
                v2.isAbsoluteTimeValue(asecs2);
                asecs2.setValue(asecs1.getTimeInMillis() + asecs2.getTimeInMillis());
                result.setAbsoluteTimeValue(asecs2);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            if (vt1 == ValueType.RELATIVE_TIME_VALUE && vt2 == ValueType.RELATIVE_TIME_VALUE) {
                v1.isRelativeTimeValue(asecs1);
                v2.isRelativeTimeValue(asecs2);
                result.setRelativeTimeValue(asecs1.plus(asecs2.getRelativeTime(), false));
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }
        }

        if (op == OpKind_SUBTRACTION_OP) {
            if (vt1 == ValueType.ABSOLUTE_TIME_VALUE && vt2 == ValueType.ABSOLUTE_TIME_VALUE) {
                v1.isAbsoluteTimeValue(asecs1);
                v2.isAbsoluteTimeValue(asecs2);
                result.setRelativeTimeValue(asecs1.subtract(asecs2, false));
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            if (vt1 == ValueType.ABSOLUTE_TIME_VALUE && vt2 == ValueType.RELATIVE_TIME_VALUE) {
                v1.isAbsoluteTimeValue(asecs1);
                v2.isRelativeTimeValue(asecs2);
                asecs1.setValue(asecs1.getTimeInMillis() - asecs2.getRelativeTime());
                result.setAbsoluteTimeValue(asecs1);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            if (vt1 == ValueType.RELATIVE_TIME_VALUE && vt2 == ValueType.RELATIVE_TIME_VALUE) {
                v1.isRelativeTimeValue(asecs1);
                v2.isRelativeTimeValue(asecs2);
                result.setRelativeTimeValue(asecs1.subtract(asecs2));
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }
        }

        if (op == OpKind_MULTIPLICATION_OP || op == OpKind_DIVISION_OP) {
            if (vt1 == ValueType.RELATIVE_TIME_VALUE && vt2 == ValueType.INTEGER_VALUE) {
                AMutableInt64 num = objectPool.int64Pool.get();
                ClassAdTime msecs = objectPool.classAdTimePool.get();
                v1.isRelativeTimeValue(asecs1);
                v2.isIntegerValue(num);
                if (op == OpKind_MULTIPLICATION_OP) {
                    msecs.setValue(asecs1.multiply(num.getLongValue(), false));
                } else {
                    msecs.setValue(asecs1.divide(num.getLongValue(), false));
                }
                result.setRelativeTimeValue(msecs);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            if (vt1 == ValueType.RELATIVE_TIME_VALUE && vt2 == ValueType.REAL_VALUE) {
                AMutableDouble num = objectPool.doublePool.get();
                AMutableDouble msecs = objectPool.doublePool.get();
                v1.isRelativeTimeValue(asecs1);
                v2.isRealValue(num);
                if (op == OpKind_MULTIPLICATION_OP) {
                    msecs.setValue(asecs1.getRelativeTime() * num.getDoubleValue());
                } else {
                    msecs.setValue(asecs1.getRelativeTime() * num.getDoubleValue());
                }
                ClassAdTime time = objectPool.classAdTimePool.get();
                time.setRelativeTime(1000L * ((long) msecs.getDoubleValue()));
                result.setRelativeTimeValue(time);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            if (vt1 == ValueType.INTEGER_VALUE && vt2 == ValueType.RELATIVE_TIME_VALUE
                    && op == OpKind_MULTIPLICATION_OP) {
                AMutableInt64 num = objectPool.int64Pool.get();
                v1.isIntegerValue(num);
                v2.isRelativeTimeValue(asecs1);
                ClassAdTime time = objectPool.classAdTimePool.get();
                time.setRelativeTime(num.getLongValue() * asecs1.getRelativeTime());
                result.setRelativeTimeValue(time);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }

            if (vt2 == ValueType.RELATIVE_TIME_VALUE && vt1 == ValueType.REAL_VALUE && op == OpKind_MULTIPLICATION_OP) {
                AMutableDouble num = objectPool.doublePool.get();
                v1.isRelativeTimeValue(asecs1);
                v2.isRealValue(num);
                ClassAdTime time = objectPool.classAdTimePool.get();
                time.setRelativeTime((long) (asecs1.getRelativeTime() * num.getDoubleValue()));
                result.setRelativeTimeValue(time);
                return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
            }
        }
        // no other operations are supported on times
        result.setErrorValue();
        return (SigValues.SIG_CHLD1.ordinal() | SigValues.SIG_CHLD2.ordinal());
    }

    public static void compareStrings(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool) {
        AMutableCharArrayString s1 = objectPool.strPool.get();
        AMutableCharArrayString s2 = objectPool.strPool.get();
        int cmp;
        v1.isStringValue(s1);
        v2.isStringValue(s2);
        result.setBooleanValue(false);
        if (op == OpKind_META_EQUAL_OP || op == OpKind_META_NOT_EQUAL_OP) {
            cmp = s1.compareTo(s2);
        } else {
            cmp = s1.compareToIgnoreCase(s2);
        }
        if (cmp < 0) {
            // s1 < s2
            if (op == OpKind_LESS_THAN_OP || op == OpKind_LESS_OR_EQUAL_OP || op == OpKind_META_NOT_EQUAL_OP
                    || op == OpKind_NOT_EQUAL_OP) {
                result.setBooleanValue(true);
            }
        } else if (cmp == 0) {
            // s1 == s2
            if (op == OpKind_LESS_OR_EQUAL_OP || op == OpKind_META_EQUAL_OP || op == OpKind_EQUAL_OP
                    || op == OpKind_GREATER_OR_EQUAL_OP) {
                result.setBooleanValue(true);
            }
        } else {
            // s1 > s2
            if (op == OpKind_GREATER_THAN_OP || op == OpKind_GREATER_OR_EQUAL_OP || op == OpKind_META_NOT_EQUAL_OP
                    || op == OpKind_NOT_EQUAL_OP) {
                result.setBooleanValue(true);
            }
        }
    }

    public static void compareAbsoluteTimes(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        ClassAdTime asecs1 = objectPool.classAdTimePool.get();
        ClassAdTime asecs2 = objectPool.classAdTimePool.get();
        boolean compResult = false;
        v1.isAbsoluteTimeValue(asecs1);
        v2.isAbsoluteTimeValue(asecs2);
        switch (op) {
            case OpKind_LESS_THAN_OP:
                compResult = (asecs1.getTimeInMillis() < asecs2.getTimeInMillis());
                break;
            case OpKind_LESS_OR_EQUAL_OP:
                compResult = (asecs1.getTime() <= asecs2.getTime());
                break;
            case OpKind_EQUAL_OP:
                compResult = (asecs1.getTime() == asecs2.getTime());
                break;
            case OpKind_META_EQUAL_OP:
                compResult = (asecs1.getTime() == asecs2.getTime()) && (asecs1.getOffset() == asecs2.getOffset());
                break;
            case OpKind_NOT_EQUAL_OP:
                compResult = (asecs1.getTime() != asecs2.getTime());
                break;
            case OpKind_META_NOT_EQUAL_OP:
                compResult = (asecs1.getTime() != asecs2.getTime()) || (asecs1.getOffset() != asecs2.getOffset());
                break;
            case OpKind_GREATER_THAN_OP:
                compResult = (asecs1.getTime() > asecs2.getTime());
                break;
            case OpKind_GREATER_OR_EQUAL_OP:
                compResult = (asecs1.getTime() >= asecs2.getTime());
                break;
            default:
                // should not get here
                throw new HyracksDataException("Should not get here");
        }
        result.setBooleanValue(compResult);
    }

    public static void compareRelativeTimes(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        ClassAdTime rsecs1 = objectPool.classAdTimePool.get();
        ClassAdTime rsecs2 = objectPool.classAdTimePool.get();
        boolean compResult = false;

        v1.isRelativeTimeValue(rsecs1);
        v2.isRelativeTimeValue(rsecs2);

        switch (op) {
            case OpKind_LESS_THAN_OP:
                compResult = (rsecs1.getRelativeTime() < rsecs2.getRelativeTime());
                break;

            case OpKind_LESS_OR_EQUAL_OP:
                compResult = (rsecs1.getRelativeTime() <= rsecs2.getRelativeTime());
                break;

            case OpKind_EQUAL_OP:
            case OpKind_META_EQUAL_OP:
                compResult = (rsecs1.getRelativeTime() == rsecs2.getRelativeTime());
                break;

            case OpKind_NOT_EQUAL_OP:
            case OpKind_META_NOT_EQUAL_OP:
                compResult = (rsecs1.getRelativeTime() != rsecs2.getRelativeTime());
                break;

            case OpKind_GREATER_THAN_OP:
                compResult = (rsecs1.getRelativeTime() > rsecs2.getRelativeTime());
                break;

            case OpKind_GREATER_OR_EQUAL_OP:
                compResult = (rsecs1.getRelativeTime() >= rsecs2.getRelativeTime());
                break;

            default:
                // should not get here
                throw new HyracksDataException("Should not get here");
        }
        result.setBooleanValue(compResult);
    }

    public static void compareBools(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        MutableBoolean b1 = objectPool.boolPool.get();
        MutableBoolean b2 = objectPool.boolPool.get();
        boolean compResult = false;
        v1.isBooleanValue(b1);
        v2.isBooleanValue(b2);

        switch (op) {
            case OpKind_LESS_THAN_OP:
                compResult = (b1.compareTo(b2) < 0);
                break;
            case OpKind_LESS_OR_EQUAL_OP:
                compResult = (b1.compareTo(b2) <= 0);
                break;
            case OpKind_EQUAL_OP:
                compResult = (b1.booleanValue() == b2.booleanValue());
                break;
            case OpKind_META_EQUAL_OP:
                compResult = (b1.booleanValue() == b2.booleanValue());
                break;
            case OpKind_NOT_EQUAL_OP:
                compResult = (b1.booleanValue() != b2.booleanValue());
                break;
            case OpKind_META_NOT_EQUAL_OP:
                compResult = (b1.booleanValue() != b2.booleanValue());
                break;
            case OpKind_GREATER_THAN_OP:
                compResult = (b1.compareTo(b2) > 0);
                break;
            case OpKind_GREATER_OR_EQUAL_OP:
                compResult = (b1.compareTo(b2) >= 0);
                break;
            default:
                // should not get here
                throw new HyracksDataException("Should not get here");
        }
        result.setBooleanValue(compResult);
    }

    public static void compareIntegers(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        AMutableInt64 i1 = objectPool.int64Pool.get();
        AMutableInt64 i2 = objectPool.int64Pool.get();
        boolean compResult = false;
        v1.isIntegerValue(i1);
        v2.isIntegerValue(i2);
        switch (op) {
            case OpKind_LESS_THAN_OP:
                compResult = (i1.getLongValue() < i2.getLongValue());
                break;
            case OpKind_LESS_OR_EQUAL_OP:
                compResult = (i1.getLongValue() <= i2.getLongValue());
                break;
            case OpKind_EQUAL_OP:
                compResult = (i1.getLongValue() == i2.getLongValue());
                break;
            case OpKind_META_EQUAL_OP:
                compResult = (i1.getLongValue() == i2.getLongValue());
                break;
            case OpKind_NOT_EQUAL_OP:
                compResult = (i1.getLongValue() != i2.getLongValue());
                break;
            case OpKind_META_NOT_EQUAL_OP:
                compResult = (i1.getLongValue() != i2.getLongValue());
                break;
            case OpKind_GREATER_THAN_OP:
                compResult = (i1.getLongValue() > i2.getLongValue());
                break;
            case OpKind_GREATER_OR_EQUAL_OP:
                compResult = (i1.getLongValue() >= i2.getLongValue());
                break;
            default:
                // should not get here
                throw new HyracksDataException("Should not get here");
        }
        result.setBooleanValue(compResult);
    }

    public static void compareReals(int op, Value v1, Value v2, Value result, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        AMutableDouble r1 = objectPool.doublePool.get();
        AMutableDouble r2 = objectPool.doublePool.get();
        boolean compResult = false;

        v1.isRealValue(r1);
        v2.isRealValue(r2);

        switch (op) {
            case OpKind_LESS_THAN_OP:
                compResult = (r1.getDoubleValue() < r2.getDoubleValue());
                break;
            case OpKind_LESS_OR_EQUAL_OP:
                compResult = (r1.getDoubleValue() <= r2.getDoubleValue());
                break;
            case OpKind_EQUAL_OP:
                compResult = (r1.getDoubleValue() == r2.getDoubleValue());
                break;
            case OpKind_META_EQUAL_OP:
                compResult = (r1.getDoubleValue() == r2.getDoubleValue());
                break;
            case OpKind_NOT_EQUAL_OP:
                compResult = (r1.getDoubleValue() != r2.getDoubleValue());
                break;
            case OpKind_META_NOT_EQUAL_OP:
                compResult = (r1.getDoubleValue() != r2.getDoubleValue());
                break;
            case OpKind_GREATER_THAN_OP:
                compResult = (r1.getDoubleValue() > r2.getDoubleValue());
                break;
            case OpKind_GREATER_OR_EQUAL_OP:
                compResult = (r1.getDoubleValue() >= r2.getDoubleValue());
                break;
            default:
                // should not get here
                throw new HyracksDataException("Should not get here");
        }
        result.setBooleanValue(compResult);
    }

    // This function performs type promotions so that both v1 and v2 are of the
    // same numerical type: (v1 and v2 are not ERROR or UNDEFINED)
    //  + if both v1 and v2 are Numbers and of the same type, return type
    //  + if v1 is an int and v2 is a real, convert v1 to real; return REAL_VALUE
    //  + if v1 is a real and v2 is an int, convert v2 to real; return REAL_VALUE
    public static ValueType coerceToNumber(Value v1, Value v2, ClassAdObjectPool objectPool) {
        AMutableInt64 i = objectPool.int64Pool.get();
        AMutableDouble r = objectPool.doublePool.get();
        MutableBoolean b = objectPool.boolPool.get();

        // either of v1, v2 not numerical?
        if (v1.isClassAdValue() || v2.isClassAdValue()) {
            return ValueType.CLASSAD_VALUE;
        }
        if (v1.isListValue() || v2.isListValue()) {
            return ValueType.LIST_VALUE;
        }
        if (v1.isStringValue() || v2.isStringValue()) {
            return ValueType.STRING_VALUE;
        }
        if (v1.isUndefinedValue() || v2.isUndefinedValue()) {
            return ValueType.UNDEFINED_VALUE;
        }
        if (v1.isErrorValue() || v2.isErrorValue()) {
            return ValueType.ERROR_VALUE;
        }
        if (v1.isAbsoluteTimeValue() || v2.isAbsoluteTimeValue()) {
            return ValueType.ABSOLUTE_TIME_VALUE;
        }
        if (v1.isRelativeTimeValue() || v2.isRelativeTimeValue()) {
            return ValueType.RELATIVE_TIME_VALUE;
        }

        // promote booleans to integers
        if (v1.isBooleanValue(b)) {
            if (b.booleanValue()) {
                v1.setIntegerValue(1);
            } else {
                v1.setIntegerValue(0);
            }
        }

        if (v2.isBooleanValue(b)) {
            if (b.booleanValue()) {
                v2.setIntegerValue(1);
            } else {
                v2.setIntegerValue(0);
            }
        }

        // both v1 and v2 of same numerical type
        if (v1.isIntegerValue(i) && v2.isIntegerValue(i)) {
            return ValueType.INTEGER_VALUE;
        }
        if (v1.isRealValue(r) && v2.isRealValue(r)) {
            return ValueType.REAL_VALUE;
        }

        // type promotions required
        if (v1.isIntegerValue(i) && v2.isRealValue(r)) {
            v1.setRealValue(i.getLongValue());
        } else if (v1.isRealValue(r) && v2.isIntegerValue(i)) {
            v2.setRealValue(i.getLongValue());
        }

        return ValueType.REAL_VALUE;
    }

    public Operation(int op, ExprTreeHolder e1, ExprTreeHolder e2, ExprTreeHolder e3, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        super(objectPool);
        this.opKind = op;
        this.child1 = new ExprTreeHolder(objectPool);
        this.child2 = new ExprTreeHolder(objectPool);
        this.child3 = new ExprTreeHolder(objectPool);
        child1.copyFrom(e1);
        child2.copyFrom(e2);
        child3.copyFrom(e3);
    }

    public static Operation createOperation(int op, ExprTree e1, ExprTree e2, ExprTree e3, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        Operation opnode = objectPool.operationPool.get();
        opnode.opKind = op;
        opnode.child1.copyFrom(e1);
        opnode.child2.copyFrom(e2);
        opnode.child3.copyFrom(e3);
        return opnode;
    }

    public static void createOperation(int op, ExprTree e1, ExprTree e2, ExprTree e3, Operation opnode)
            throws HyracksDataException {
        opnode.opKind = op;
        opnode.child1.copyFrom(e1);
        opnode.child2.copyFrom(e2);
        opnode.child3.copyFrom(e3);
    }

    public void getComponents(AMutableInt32 op, ExprTreeHolder e1, ExprTreeHolder e2, ExprTreeHolder e3) {
        op.setValue(opKind);
        e1.setInnerTree(child1);
        e2.setInnerTree(child2);
        e3.setInnerTree(child3);
    }

    public static Operation createOperation(int op, Value val, ExprTreeHolder tree, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        if (tree.getInnerTree() == null) {
            return null;
        }
        Literal lit = Literal.createLiteral(val, objectPool);
        if (lit == null) {
            return null;
        }
        Operation newOp = createOperation(op, lit, tree, objectPool);
        return newOp;
    }

    public static Operation createOperation(int op, ExprTreeHolder tree, Value val, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        if (tree.getInnerTree() == null) {
            return null;
        }
        Literal lit = Literal.createLiteral(val, objectPool);
        if (lit == null) {
            return null;
        }
        Operation newOp = createOperation(op, lit, tree, objectPool);
        return newOp;
    }

    public boolean flattenSpecials(EvalState state, Value val, ExprTreeHolder tree) throws HyracksDataException {
        ExprTreeHolder fChild1 = objectPool.mutableExprPool.get();
        ExprTreeHolder fChild2 = objectPool.mutableExprPool.get();
        ExprTreeHolder fChild3 = objectPool.mutableExprPool.get();
        Value eval1 = objectPool.valuePool.get();
        Value eval2 = objectPool.valuePool.get();
        Value eval3 = objectPool.valuePool.get();

        switch (opKind) {
            case OpKind_UNARY_PLUS_OP:
            case OpKind_UNARY_MINUS_OP:
            case OpKind_PARENTHESES_OP:
            case OpKind_LOGICAL_NOT_OP:
            case OpKind_BITWISE_NOT_OP:
                if (!child1.publicFlatten(state, eval1, fChild1)) {
                    tree.setInnerTree(null);
                    return false;
                }
                if (fChild1.getInnerTree() != null) {
                    tree.setInnerTree(Operation.createOperation(opKind, fChild1, objectPool));
                    return (tree.getInnerTree() != null);
                } else {
                    privateDoOperation(opKind, eval1, null, null, true, false, false, val, objectPool);
                    tree.setInnerTree(null);
                    eval1.setUndefinedValue();
                    return true;
                }
            case OpKind_TERNARY_OP:
                // Flatten the selector expression
                if (!child1.publicFlatten(state, eval1, fChild1)) {
                    tree.setInnerTree(null);
                    return false;
                }

                // check if selector expression collapsed to a non-undefined value
                if (fChild1.getInnerTree() == null && !eval1.isUndefinedValue()) {
                    MutableBoolean b = objectPool.boolPool.get();
                    // if the selector is not boolean-equivalent, propagate error
                    if (!eval1.isBooleanValueEquiv(b)) {
                        val.setErrorValue();
                        eval1.setUndefinedValue();
                        tree.setInnerTree(null);
                        return true;
                    }

                    // eval1 is either a real or an integer
                    if (b.booleanValue()) {
                        return child2.publicFlatten(state, val, tree);
                    } else {
                        return child3.publicFlatten(state, val, tree);
                    }
                } else {
                    // Flatten arms of the if expression
                    if (!child2.publicFlatten(state, eval2, fChild2) || !child3.publicFlatten(state, eval3, fChild3)) {
                        // clean up
                        tree.setInnerTree(null);
                        return false;
                    }

                    // if any arm collapsed into a value, make it a Literal
                    if (fChild2.getInnerTree() == null) {
                        fChild2.setInnerTree(Literal.createLiteral(eval2, objectPool));
                    }
                    if (fChild3.getInnerTree() == null) {
                        fChild3.setInnerTree(Literal.createLiteral(eval3, objectPool));
                    }
                    if (fChild2.getInnerTree() == null || fChild3.getInnerTree() == null) {
                        tree.setInnerTree(null);
                        return false;
                    }

                    // fChild1 may be NULL if child1 Flattened to UNDEFINED
                    if (fChild1.getInnerTree() == null) {
                        fChild1.setInnerTree(child1.copy());
                    }

                    tree.setInnerTree(Operation.createOperation(opKind, fChild1, fChild2, fChild3, objectPool));
                    if (tree.getInnerTree() == null) {
                        return false;
                    }
                    return true;
                }
            case OpKind_SUBSCRIPT_OP:
                // Flatten both arguments
                if (!child1.publicFlatten(state, eval1, fChild1) || !child2.publicFlatten(state, eval2, fChild2)) {
                    tree.setInnerTree(null);
                    return false;
                }

                // if both arguments Flattened to values, Evaluate now
                if (fChild1.getInnerTree() == null && fChild2.getInnerTree() == null) {
                    privateDoOperation(opKind, eval1, eval2, null, true, true, false, val, objectPool);
                    tree.setInnerTree(null);
                    return true;
                }

                // otherwise convert Flattened values into literals
                if (fChild1.getInnerTree() == null) {
                    fChild1.setInnerTree(Literal.createLiteral(eval1, objectPool));
                }
                if (fChild2.getInnerTree() == null) {
                    fChild2.setInnerTree(Literal.createLiteral(eval2, objectPool));
                }
                if (fChild1.getInnerTree() == null || fChild2.getInnerTree() == null) {
                    tree.setInnerTree(null);
                    return false;
                }

                tree.setInnerTree(Operation.createOperation(opKind, fChild1, fChild2, objectPool));
                if (tree.getInnerTree() == null) {
                    return false;
                }
                return true;

            default:
                throw new HyracksDataException("Should not get here");
        }
    }

    public static boolean isStrictOperator(int op) {
        switch (op) {
            case OpKind_META_EQUAL_OP:
            case OpKind_META_NOT_EQUAL_OP:
            case OpKind_LOGICAL_AND_OP:
            case OpKind_LOGICAL_OR_OP:
            case OpKind_TERNARY_OP:
                return false;
            default:
                return true;
        }
    }

    // get precedence levels for operators (see K&R p.53 )
    public static int precedenceLevel(int op) {
        switch (op) {
            case OpKind_SUBSCRIPT_OP:
                return (12);

            case OpKind_LOGICAL_NOT_OP:
            case OpKind_BITWISE_NOT_OP:
            case OpKind_UNARY_PLUS_OP:
            case OpKind_UNARY_MINUS_OP:
                return (11);

            case OpKind_MULTIPLICATION_OP:
            case OpKind_DIVISION_OP:
            case OpKind_MODULUS_OP:
                return (10);

            case OpKind_ADDITION_OP:
            case OpKind_SUBTRACTION_OP:
                return (9);

            case OpKind_LEFT_SHIFT_OP:
            case OpKind_RIGHT_SHIFT_OP:
            case OpKind_URIGHT_SHIFT_OP:
                return (8);

            case OpKind_LESS_THAN_OP:
            case OpKind_LESS_OR_EQUAL_OP:
            case OpKind_GREATER_OR_EQUAL_OP:
            case OpKind_GREATER_THAN_OP:
                return (7);

            case OpKind_NOT_EQUAL_OP:
            case OpKind_EQUAL_OP:
            case OpKind_IS_OP:
            case OpKind_ISNT_OP:
                return (6);

            case OpKind_BITWISE_AND_OP:
                return (5);

            case OpKind_BITWISE_XOR_OP:
                return (4);

            case OpKind_BITWISE_OR_OP:
                return (3);

            case OpKind_LOGICAL_AND_OP:
                return (2);

            case OpKind_LOGICAL_OR_OP:
                return (1);

            case OpKind_TERNARY_OP:
                return (0);
            default:
                return (-1);
        }
    }

    @Override
    public void reset() {
        opKind = OpKind_NO_OP;
        child1.reset();
        child2.reset();
        child3.reset();
    }
}
