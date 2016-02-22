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

import org.apache.asterix.om.base.AMutableInt32;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AttributeReference extends ExprTree {

    private ExprTree expr;
    private boolean absolute;
    private AMutableCharArrayString attributeStr;
    private ClassAd current = new ClassAd(false, false);
    private ExprList adList = new ExprList();
    private Value val = new Value();
    private MutableBoolean rVal = new MutableBoolean(false);
    private AttributeReference tempAttrRef;
    private EvalState tstate = new EvalState();

    public ExprTree getExpr() {
        return expr;
    }

    public void setExpr(ExprTree expr) {
        this.expr = expr == null ? null : expr.self();
    }

    public AttributeReference() {
        expr = null;
        attributeStr = null;
        absolute = false;
    }

    /// Copy Constructor
    public AttributeReference(AttributeReference ref) throws HyracksDataException {
        copyFrom(ref);
    }

    /// Assignment operator
    @Override
    public boolean equals(Object o) {
        if (o instanceof AttributeReference) {
            AttributeReference ref = (AttributeReference) o;
            return sameAs(ref);
        }
        return false;
    }

    /// node type
    @Override
    public NodeKind getKind() {
        return NodeKind.ATTRREF_NODE;
    }

    public static AttributeReference createAttributeReference(ExprTree expr, AMutableCharArrayString attrName) {
        return createAttributeReference(expr, attrName, false);
    }

    /**
     * Return a copy of this attribute reference.
     *
     * @throws HyracksDataException
     */
    @Override
    public ExprTree copy() throws HyracksDataException {
        AttributeReference newTree = new AttributeReference();
        newTree.copyFrom(this);
        return newTree;
    }

    /**
     * Copy from the given reference into this reference.
     *
     * @param ref
     *            The reference to copy from.
     * @return true if the copy succeeded, false otherwise.
     * @throws HyracksDataException
     */
    public boolean copyFrom(AttributeReference ref) throws HyracksDataException {
        if (attributeStr == null) {
            attributeStr = new AMutableCharArrayString(ref.attributeStr);
        } else {
            attributeStr.setValue(ref.attributeStr);
        }
        if (ref.expr != null) {
            expr = ref.expr.copy();
        }
        super.copyFrom(ref);
        this.absolute = ref.absolute;
        return true;
    }

    /**
     * Is this attribute reference the same as another?
     *
     * @param tree
     *            The reference to compare with
     * @return true if they are the same, false otherwise.
     */
    @Override
    public boolean sameAs(ExprTree tree) {
        boolean is_same;
        ExprTree pSelfTree = tree.self();
        if (this == pSelfTree) {
            is_same = true;
        } else if (pSelfTree.getKind() != NodeKind.ATTRREF_NODE) {
            is_same = false;
        } else {
            AttributeReference other_ref = (AttributeReference) pSelfTree;
            if (absolute != other_ref.absolute || !attributeStr.equals(other_ref.attributeStr)) {
                is_same = false;
            } else if ((expr == null && other_ref.expr == null) || (expr.equals(other_ref.expr))
                    || (expr != null && other_ref.expr != null && ((AttributeReference) expr).sameAs(other_ref.expr))) {
                // Will this check result in infinite recursion? How do I stop it?
                is_same = true;
            } else {
                is_same = false;
            }
        }
        return is_same;
    }

    // a private ctor for use in significant expr identification
    private AttributeReference(ExprTree tree, AMutableCharArrayString attrname, boolean absolut) {
        attributeStr = attrname;
        expr = tree == null ? null : tree.self();
        absolute = absolut;
    }

    @Override
    public void privateSetParentScope(ClassAd parent) {
        if (expr != null) {
            expr.setParentScope(parent);
        }
    }

    public void getComponents(ExprTreeHolder tree, AMutableCharArrayString attr, MutableBoolean abs)
            throws HyracksDataException {
        tree.copyFrom(expr);
        attr.setValue(attributeStr);
        abs.setValue(absolute);
    }

    public EvalResult findExpr(EvalState state, ExprTreeHolder tree, ExprTreeHolder sig, boolean wantSig)
            throws HyracksDataException {
        // establish starting point for search
        if (expr == null) {
            // "attr" and ".attr"
            current = absolute ? state.getRootAd() : state.getCurAd();
            if (absolute && (current == null)) { // NAC - circularity so no root
                return EvalResult.EVAL_FAIL; // NAC
            } // NAC
        } else {
            // "expr.attr"
            rVal.setValue(wantSig ? expr.publicEvaluate(state, val, sig) : expr.publicEvaluate(state, val));
            if (!rVal.booleanValue()) {
                return (EvalResult.EVAL_FAIL);
            }

            if (val.isUndefinedValue()) {
                return (EvalResult.EVAL_UNDEF);
            } else if (val.isErrorValue()) {
                return (EvalResult.EVAL_ERROR);
            }

            if (!val.isClassAdValue(current) && !val.isListValue(adList)) {
                return (EvalResult.EVAL_ERROR);
            }
        }

        if (val.isListValue()) {
            ExprList eList = new ExprList();
            //
            // iterate through exprList and apply attribute reference
            // to each exprTree
            for (ExprTree currExpr : adList.getExprList()) {
                if (currExpr == null) {
                    return (EvalResult.EVAL_FAIL);
                } else {
                    if (tempAttrRef == null) {
                        tempAttrRef = new AttributeReference();
                    } else {
                        tempAttrRef.reset();
                    }
                    createAttributeReference(currExpr.copy(), attributeStr, false, tempAttrRef);
                    val.clear();
                    // Create new EvalState, within this scope, because
                    // attrRef is only temporary, so we do not want to
                    // cache the evaluated result in the outer state object.
                    tstate.reset();
                    tstate.setScopes(state.getCurAd());
                    rVal.setValue(wantSig ? tempAttrRef.publicEvaluate(tstate, val, sig)
                            : tempAttrRef.publicEvaluate(tstate, val));
                    if (!rVal.booleanValue()) {
                        return (EvalResult.EVAL_FAIL);
                    }

                    ClassAd evaledAd = new ClassAd();
                    ExprList evaledList = new ExprList();
                    if (val.isClassAdValue(evaledAd)) {
                        eList.add(evaledAd);
                        continue;
                    } else if (val.isListValue(evaledList)) {
                        eList.add(evaledList.copy());
                        continue;
                    } else {
                        eList.add(Literal.createLiteral(val));
                    }
                }
            }
            tree.setInnerTree(ExprList.createExprList(eList));
            ClassAd newRoot = new ClassAd();
            tree.setParentScope(newRoot);
            return EvalResult.EVAL_OK;
        }
        // lookup with scope; this may side-affect state

        /* ClassAd::alternateScope is intended for transitioning Condor from
         * old to new ClassAds. It allows unscoped attribute references
         * in expressions that can't be found in the local scope to be
         * looked for in an alternate scope. In Condor, the alternate
         * scope is the Target ad in matchmaking.
         * Expect alternateScope to be removed from a future release.
         */
        if (current == null) {
            return EvalResult.EVAL_UNDEF;
        }
        int rc = current.lookupInScope(attributeStr.toString(), tree, state);
        if (expr == null && !absolute && rc == EvalResult.EVAL_UNDEF.ordinal() && current.getAlternateScope() != null) {
            rc = current.getAlternateScope().lookupInScope(attributeStr.toString(), tree, state);
        }
        return EvalResult.values()[rc];
    }

    @Override
    public boolean publicEvaluate(EvalState state, Value val) throws HyracksDataException {
        ExprTreeHolder tree = new ExprTreeHolder();
        ExprTreeHolder dummy = new ExprTreeHolder();
        ClassAd curAd = new ClassAd(state.getCurAd());
        boolean rval;
        // find the expression and the evalstate
        switch (findExpr(state, tree, dummy, false)) {
            case EVAL_FAIL:
                return false;
            case EVAL_ERROR:
                val.setErrorValue();
                state.setCurAd(curAd);
                return true;
            case EVAL_UNDEF:
                val.setUndefinedValue();
                state.setCurAd(curAd);
                return true;
            case EVAL_OK: {
                if (state.getDepthRemaining() <= 0) {
                    val.setErrorValue();
                    state.setCurAd(curAd);
                    return false;
                }
                state.decrementDepth();
                rval = tree.publicEvaluate(state, val);
                state.incrementDepth();
                state.getCurAd().setValue(curAd);
                return rval;
            }
            default:
                throw new HyracksDataException("ClassAd:  Should not reach here");
        }
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val, ExprTreeHolder sig) throws HyracksDataException {
        ExprTreeHolder tree = new ExprTreeHolder();
        ExprTreeHolder exprSig = new ExprTreeHolder();
        ClassAd curAd = new ClassAd(state.getCurAd());
        MutableBoolean rval = new MutableBoolean(true);
        switch (findExpr(state, tree, exprSig, true)) {
            case EVAL_FAIL:
                rval.setValue(false);
                break;
            case EVAL_ERROR:
                val.setErrorValue();
                break;
            case EVAL_UNDEF:
                val.setUndefinedValue();
                break;
            case EVAL_OK: {
                if (state.getDepthRemaining() <= 0) {
                    val.setErrorValue();
                    state.getCurAd().setValue(curAd);
                    return false;
                }
                state.decrementDepth();
                rval.setValue(tree.publicEvaluate(state, val));
                state.incrementDepth();
                break;
            }
            default:
                throw new HyracksDataException("ClassAd:  Should not reach here");
        }
        sig.setInnerTree((new AttributeReference(exprSig, attributeStr, absolute)));
        state.getCurAd().setValue(curAd);
        return rval.booleanValue();
    }

    @Override
    public boolean privateFlatten(EvalState state, Value val, ExprTreeHolder ntree, AMutableInt32 op)
            throws HyracksDataException {
        ExprTreeHolder tree = new ExprTreeHolder();
        ExprTreeHolder dummy = new ExprTreeHolder();
        ClassAd curAd;
        boolean rval;
        ntree.setInnerTree(null); // Just to be safe...  wenger 2003-12-11.
        // find the expression and the evalstate
        curAd = state.getCurAd();
        switch (findExpr(state, tree, dummy, false)) {
            case EVAL_FAIL:
                return false;
            case EVAL_ERROR:
                val.setErrorValue();
                state.getCurAd().setValue(curAd);
                return true;
            case EVAL_UNDEF:
                if (expr != null && state.isFlattenAndInline()) {
                    ExprTreeHolder expr_ntree = new ExprTreeHolder();
                    Value expr_val = new Value();
                    if (state.getDepthRemaining() <= 0) {
                        val.setErrorValue();
                        state.getCurAd().setValue(curAd);
                        return false;
                    }
                    state.decrementDepth();
                    rval = expr.publicFlatten(state, expr_val, expr_ntree);
                    state.incrementDepth();
                    if (rval && expr_ntree.getInnerTree() != null) {
                        ntree.setInnerTree(createAttributeReference(expr_ntree, attributeStr));
                        if (ntree.getInnerTree() != null) {
                            state.getCurAd().setValue(curAd);
                            return true;
                        }
                    }
                }
                ntree.setInnerTree(copy());
                state.getCurAd().setValue(curAd);
                return true;
            case EVAL_OK: {
                // Don't flatten or inline a classad that's referred to
                // by an attribute.
                if (tree.getKind() == NodeKind.CLASSAD_NODE) {
                    ntree.setInnerTree(copy());
                    val.setUndefinedValue();
                    return true;
                }

                if (state.getDepthRemaining() <= 0) {
                    val.setErrorValue();
                    state.getCurAd().setValue(curAd);
                    return false;
                }
                state.decrementDepth();

                rval = tree.publicFlatten(state, val, ntree);
                state.incrementDepth();

                // don't inline if it didn't flatten to a value, and clear cache
                // do inline if FlattenAndInline was called
                if (ntree.getInnerTree() != null) {
                    if (state.isFlattenAndInline()) { // NAC
                        return true; // NAC
                    } // NAC
                    ntree.setInnerTree(copy());
                    val.setUndefinedValue();
                }

                state.getCurAd().setValue(curAd);
                return rval;
            }
            default:
                throw new HyracksDataException("ClassAd:  Should not reach here");
        }
    }

    /**
     * Factory method to create attribute reference nodes.
     *
     * @param expr
     *            The expression part of the reference (i.e., in
     *            case of expr.attr). This parameter is NULL if the reference
     *            is absolute (i.e., .attr) or simple (i.e., attr).
     * @param attrName
     *            The name of the reference. This string is
     *            duplicated internally.
     * @param absolute
     *            True if the reference is an absolute reference
     *            (i.e., in case of .attr). This parameter cannot be true if
     *            expr is not NULL, default value is false;
     */
    public static AttributeReference createAttributeReference(ExprTree tree, AMutableCharArrayString attrStr,
            boolean absolut) {
        return (new AttributeReference(tree, attrStr, absolut));
    }

    public void setValue(ExprTree tree, AMutableCharArrayString attrStr, boolean absolut) {
        this.absolute = absolut;
        this.attributeStr = attrStr;
        this.expr = tree == null ? null : tree.self();
    }

    public static void createAttributeReference(ExprTree tree, AMutableCharArrayString attrStr, boolean absolut,
            AttributeReference ref) {
        ref.setValue(tree, attrStr, absolut);
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val) throws HyracksDataException {
        ExprTreeHolder tree = new ExprTreeHolder();
        ExprTreeHolder dummy = new ExprTreeHolder();
        ClassAd curAd;
        boolean rval;

        // find the expression and the evalstate
        curAd = state.getCurAd();
        switch (findExpr(state, tree, dummy, false)) {
            case EVAL_FAIL:
                return false;
            case EVAL_ERROR:
                val.setErrorValue();
                state.getCurAd().setValue(curAd);
                return true;
            case EVAL_UNDEF:
                val.setUndefinedValue();
                state.getCurAd().setValue(curAd);
                return true;

            case EVAL_OK: {
                if (state.getDepthRemaining() <= 0) {
                    val.setErrorValue();
                    state.getCurAd().setValue(curAd);
                    return false;
                }
                state.decrementDepth();
                rval = tree.publicEvaluate(state, val);
                state.incrementDepth();
                state.getCurAd().setValue(curAd);
                return rval;
            }
            default:
                throw new HyracksDataException("ClassAd:  Should not reach here");
        }
    }

    @Override
    public void reset() {
        if (expr != null) {
            expr.reset();
        }
    }
}
