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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.asterix.external.classad.Value.NumberFactor;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.external.library.ClassAdParser;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ClassAd extends ExprTree {

    /*
     * Static Variables
     */
    public static final int ERR_OK = 0;
    public static final int ERR_MEM_ALLOC_FAILED = 1;
    public static final int ERR_BAD_VALUE = 255;
    public static final int ERR_FAILED_SET_VIEW_NAME = 256;
    public static final int ERR_NO_RANK_EXPR = 257;
    public static final int ERR_NO_REQUIREMENTS_EXPR = 258;
    public static final int ERR_BAD_PARTITION_EXPRS = 259;
    public static final int ERR_PARTITION_EXISTS = 260;
    public static final int ERR_MISSING_ATTRNAME = 261;
    public static final int ERR_BAD_EXPRESSION = 262;
    public static final int ERR_INVALID_IDENTIFIER = 263;
    public static final int ERR_MISSING_ATTRIBUTE = 264;
    public static final int ERR_NO_SUCH_VIEW = 265;
    public static final int ERR_VIEW_PRESENT = 266;
    public static final int ERR_TRANSACTION_EXISTS = 267;
    public static final int ERR_NO_SUCH_TRANSACTION = 268;
    public static final int ERR_NO_REPRESENTATIVE = 269;
    public static final int ERR_NO_PARENT_VIEW = 270;
    public static final int ERR_BAD_VIEW_INFO = 271;
    public static final int ERR_BAD_TRANSACTION_STATE = 272;
    public static final int ERR_NO_SUCH_CLASSAD = 273;
    public static final int ERR_BAD_CLASSAD = 275;
    public static final int ERR_NO_KEY = 276;
    public static final int ERR_LOG_OPEN_FAILED = 277;
    public static final int ERR_BAD_LOG_FILENAME = 278;
    public static final int ERR_NO_VIEW_NAME = 379;
    public static final int ERR_RENAME_FAILED = 280;
    public static final int ERR_NO_TRANSACTION_NAME = 281;
    public static final int ERR_PARSE_ERROR = 282;
    public static final int ERR_INTERNAL_CACHE_ERROR = 283;
    public static final int ERR_FILE_WRITE_FAILED = 284;
    public static final int ERR_FATAL_ERROR = 285;
    public static final int ERR_CANNOT_CHANGE_MODE = 286;
    public static final int ERR_CONNECT_FAILED = 287;
    public static final int ERR_CLIENT_NOT_CONNECTED = 288;
    public static final int ERR_COMMUNICATION_ERROR = 289;
    public static final int ERR_BAD_CONNECTION_TYPE = 290;
    public static final int ERR_BAD_SERVER_ACK = 291;
    public static final int ERR_CANNOT_REPLACE = 292;
    public static final int ERR_CACHE_SWITCH_ERROR = 293;
    public static final int ERR_CACHE_FILE_ERROR = 294;
    public static final int ERR_CACHE_CLASSAD_ERROR = 295;
    public static final int ERR_CANT_LOAD_DYNAMIC_LIBRARY = 296;
    public static final String ATTR_TOPLEVEL = "toplevel";
    public static final String ATTR_ROOT = "root";
    public static final String ATTR_SELF = "self";
    public static final String ATTR_PARENT = "parent";
    // The two names below are for compatibility
    public static final String ATTR_MY = "my";
    public static final String ATTR_CURRENT_TIME = "CurrentTime";
    // These versions are actually taken from an external file in the original cpp source code
    private static final int CLASSAD_VERSION_MAJOR = 8;
    private static final int CLASSAD_VERSION_MINOR = 0;
    private static final int CLASSAD_VERSION_PATCH = 0;
    private static final String CLASSAD_VERSION = "8.0.0";
    public static final ArrayList<String> specialAttrNames = new ArrayList<String>();

    static {
        specialAttrNames.add(ATTR_TOPLEVEL);
        specialAttrNames.add(ATTR_ROOT);
        specialAttrNames.add(ATTR_SELF);
        specialAttrNames.add(ATTR_PARENT);
    }

    public static final FunctionCall curr_time_expr =
            FunctionCall.createFunctionCall("time", new ExprList(new ClassAdObjectPool()), new ClassAdObjectPool());

    private ClassAd alternateScope;
    private final Map<CaseInsensitiveString, ExprTree> attrList;
    private ClassAd chainedParentAd;
    private ClassAdParser parser = null;
    private ClassAd newAd;

    /*
     * Constructors
     */
    public ClassAd(ClassAdObjectPool objectPool) {
        super(objectPool);
        parser = new ClassAdParser(this.objectPool);
        attrList = new HashMap<CaseInsensitiveString, ExprTree>();
    }

    @Override
    public void reset() {
        clear();
    }

    public boolean isReset() {
        return false;
    }

    public ClassAd getAlternateScope() {
        return alternateScope;
    }

    public void setAlternateScope(ClassAd alternateScope) {
        this.alternateScope = alternateScope;
    }

    public Map<CaseInsensitiveString, ExprTree> getAttrList() {
        return attrList;
    }

    public void classAdLibraryVersion(AMutableInt32 major, AMutableInt32 minor, AMutableInt32 patch) {
        major.setValue(CLASSAD_VERSION_MAJOR);
        minor.setValue(CLASSAD_VERSION_MINOR);
        patch.setValue(CLASSAD_VERSION_PATCH);
    }

    public static void classAdLibraryVersion(AMutableString version_string) {
        version_string.setValue(CLASSAD_VERSION);
    }

    public static ArrayList<String> getSpecialAttrNames() {
        return specialAttrNames;
    }

    public static FunctionCall getCurrentTimeExpr() {
        return curr_time_expr;
    }

    public boolean copyFrom(ClassAd ad) throws HyracksDataException {

        boolean succeeded = true;
        if (this == ad) {
            succeeded = false;
        } else {
            clear();
            // copy scoping attributes
            super.copyFrom(ad);
            chainedParentAd = ad.chainedParentAd;
            alternateScope = ad.alternateScope;
            for (Entry<CaseInsensitiveString, ExprTree> attr : ad.attrList.entrySet()) {
                ExprTree tree = objectPool.mutableExprPool.get();
                CaseInsensitiveString key = objectPool.caseInsensitiveStringPool.get();
                tree.copyFrom(attr.getValue());
                key.set(attr.getKey().get());
                attrList.put(key, tree);
            }
        }
        return succeeded;
    }

    public boolean update(ClassAd ad) throws HyracksDataException {
        for (Entry<CaseInsensitiveString, ExprTree> attr : ad.attrList.entrySet()) {
            ExprTree tree = objectPool.mutableExprPool.get();
            CaseInsensitiveString key = objectPool.caseInsensitiveStringPool.get();
            tree.copyFrom(attr.getValue());
            key.set(attr.getKey().get());
            attrList.put(key, tree);
        }
        return true;
    }

    public boolean updateFromChain(ClassAd ad) throws HyracksDataException {
        ClassAd parent = ad.chainedParentAd;
        if (parent != null) {
            if (!updateFromChain(parent)) {
                return false;
            }
        }
        return update(ad);
    }

    public boolean copyFromChain(ClassAd ad) throws HyracksDataException {
        if (this == ad) {
            return false;
        }
        clear();
        super.copyFrom(ad);
        return updateFromChain(ad);
    }

    @Override
    public boolean sameAs(ExprTree tree) {
        boolean is_same;
        ExprTree pSelfTree = tree.self();

        if (this == pSelfTree) {
            is_same = true;
        } else if (pSelfTree.getKind() != NodeKind.CLASSAD_NODE) {
            is_same = false;
        } else {
            ClassAd other_classad;
            other_classad = (ClassAd) pSelfTree;

            if (attrList.size() != other_classad.attrList.size()) {
                is_same = false;
            } else {
                is_same = true;

                for (Entry<CaseInsensitiveString, ExprTree> attr : attrList.entrySet()) {
                    ExprTree this_tree = attr.getValue();
                    ExprTree other_tree = other_classad.lookup(attr.getKey());
                    if (other_tree == null) {
                        is_same = false;
                        break;
                    } else if (!this_tree.sameAs(other_tree)) {
                        is_same = false;
                        break;
                    }
                }
            }
        }
        return is_same;
    }

    public void clear() {
        unchain();
        attrList.clear();
        if (alternateScope != null) {
            alternateScope.clear();
        }
    }

    public void unchain() {
        if (chainedParentAd != null) {
            chainedParentAd.clear();
        }
    }

    public void getComponents(Map<CaseInsensitiveString, ExprTree> attrs, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        attrs.clear();
        for (Entry<CaseInsensitiveString, ExprTree> attr : this.attrList.entrySet()) {
            ExprTree tree = objectPool.mutableExprPool.get();
            CaseInsensitiveString key = objectPool.caseInsensitiveStringPool.get();
            tree.copyFrom(attr.getValue());
            key.set(attr.getKey().get());
            attrs.put(key, tree);
        }
    }

    public ClassAd privateGetDeepScope(ExprTree tree) throws HyracksDataException {
        if (tree == null) {
            return (null);
        }
        ClassAd scope = objectPool.classAdPool.get();
        Value val = objectPool.valuePool.get();
        tree.setParentScope(this);
        if (!tree.publicEvaluate(val) || !val.isClassAdValue(scope)) {
            return (null);
        }
        return (scope);
    }

    // --- begin integer attribute insertion ----
    public boolean insertAttr(String name, int value, NumberFactor f) throws HyracksDataException {
        ExprTree plit;
        Value val = objectPool.valuePool.get();
        val.setIntegerValue(value);
        plit = Literal.createLiteral(val, f, objectPool);
        return insert(name, plit);
    }

    public boolean insertAttr(String name, int value) throws HyracksDataException {
        return insertAttr(name, value, NumberFactor.NO_FACTOR);
    }

    public boolean insertAttr(String name, long value, NumberFactor f) throws HyracksDataException {
        ExprTree plit;
        Value val = objectPool.valuePool.get();

        val.setIntegerValue(value);
        plit = Literal.createLiteral(val, f, objectPool);
        return (insert(name, plit));
    }

    public boolean insertAttr(String name, long value) throws HyracksDataException {
        return insertAttr(name, value, NumberFactor.NO_FACTOR);
    }

    public boolean deepInsertAttr(ExprTree scopeExpr, String name, int value, NumberFactor f)
            throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (false);
        }
        return (ad.insertAttr(name, value, f));
    }

    public boolean deepInsertAttr(ExprTree scopeExpr, String name, long value, NumberFactor f)
            throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (false);
        }
        return (ad.insertAttr(name, value, f));
    }

    // --- end integer attribute insertion ---

    // --- begin real attribute insertion ---
    public boolean insertAttr(String name, double value, NumberFactor f) throws HyracksDataException {
        ExprTree plit;
        Value val = objectPool.valuePool.get();
        val.setRealValue(value);
        plit = Literal.createLiteral(val, f, objectPool);
        return (insert(name, plit));
    }

    public boolean deepInsertAttr(ExprTree scopeExpr, String name, double value, NumberFactor f)
            throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (false);
        }
        return (ad.insertAttr(name, value, f));
    }

    // --- end real attribute insertion

    // --- begin boolean attribute insertion
    public boolean insertAttr(String name, boolean value) throws HyracksDataException {
        ExprTree plit;
        Value val = objectPool.valuePool.get();
        val.setBooleanValue(value);
        plit = Literal.createLiteral(val, objectPool);
        return (insert(name, plit));
    }

    public boolean deepInsertAttr(ExprTree scopeExpr, String name, boolean value) throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (false);
        }
        return (ad.insertAttr(name, value));
    }

    // --- end boolean attribute insertion

    // --- begin string attribute insertion
    public boolean insertAttr(String name, AMutableCharArrayString value) throws HyracksDataException {
        ExprTree plit;
        Value val = objectPool.valuePool.get();
        val.setStringValue(value);
        plit = Literal.createLiteral(val, objectPool);
        return (insert(name, plit));
    }

    public boolean deepInsertAttr(ExprTree scopeExpr, String name, AMutableCharArrayString value)
            throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (false);
        }
        return (ad.insertAttr(name, value));
    }

    public boolean insertAttr(String name, String value) throws HyracksDataException {
        ExprTree plit;
        Value val = objectPool.valuePool.get();

        val.setStringValue(value);
        plit = Literal.createLiteral(val, objectPool);
        return (insert(name, plit));
    }

    public boolean deepInsertAttr(ExprTree scopeExpr, String name, String value) throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (false);
        }
        return (ad.insertAttr(name, value));
    }

    // --- end string attribute insertion

    public boolean insert(String serialized_nvp) throws IOException {
        boolean bRet = false;
        String name, szValue;
        int pos, npos, vpos;
        int bpos = 0;

        // comes in as "name = value" "name= value" or "name =value"
        npos = pos = serialized_nvp.indexOf('=');

        // only try to process if the string is valid
        if (pos >= 0) {
            while (npos > 0 && serialized_nvp.charAt(npos - 1) == ' ') {
                npos--;
            }
            while (bpos < npos && serialized_nvp.charAt(bpos) == ' ') {
                bpos++;
            }
            name = serialized_nvp.substring(bpos, npos);

            vpos = pos + 1;
            while (serialized_nvp.charAt(vpos) == ' ') {
                vpos++;
            }

            szValue = serialized_nvp.substring(vpos);
            if (name.charAt(0) == '\'') {
                // We don't handle quoted attribute names for caching here.
                // Hand the name-value-pair off to the parser as a one-attribute
                // ad and merge the results into this ad.
                newAd.clear();
                name = "[" + serialized_nvp.toString() + "]";
                if (parser.parseClassAd(name, newAd, true)) {
                    return update(newAd);
                } else {
                    return false;
                }
            }

            ExprTree newTree;
            // we did not hit in the cache... parse the expression
            newTree = parser.ParseExpression(szValue);
            if (newTree != null) {
                // if caching is enabled, and we got to here then we know that the
                // cache doesn't already have an entry for this name:value, so add
                // it to the cache now.
                if (newTree.getKind() != NodeKind.LITERAL_NODE) {
                    Literal lit = objectPool.literalPool.get();
                    lit.getValue().setStringValue(szValue);
                    bRet = insert(name, lit, false);
                } else {
                    bRet = insert(name, newTree, false);
                }
            }

        } // end if pos >=0
        return bRet;
    }

    public boolean insert(String attrName, ExprTree expr) throws HyracksDataException {
        ExprTree tree = expr.copy();
        boolean result = insert(attrName, tree.isTreeHolder() ? ((ExprTreeHolder) tree).getInnerTree() : tree, false);
        return result;
    }

    public boolean insert(String attrName, ExprTree pRef, boolean cache) throws HyracksDataException {
        boolean bRet = false;
        ExprTree tree = pRef;
        // sanity checks
        if (attrName.isEmpty() || pRef == null) {
            throw new HyracksDataException("Attribute name is empty");
        }
        if (tree != null) {
            CaseInsensitiveString pstrAttr = objectPool.caseInsensitiveStringPool.get();
            pstrAttr.set(attrName);
            ExprTreeHolder mutableTree = objectPool.mutableExprPool.get();
            mutableTree.copyFrom(tree);
            // parent of the expression is this classad
            tree.setParentScope(this);
            attrList.put(pstrAttr, mutableTree);
            bRet = true;
        }
        return (bRet);
    }

    public boolean deepInsert(ExprTree scopeExpr, String name, ExprTree tree) throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (false);
        }
        return (ad.insert(name, tree));
    }

    // --- end expression insertion

    // --- begin lookup methods
    public ExprTree lookup(String name) {
        CaseInsensitiveString aString = objectPool.caseInsensitiveStringPool.get();
        aString.set(name);
        ExprTree expr = lookup(aString);
        return expr;
    }

    public ExprTree lookup(CaseInsensitiveString name) {
        /*
         * System.out.println("Lookup Printing all attributes with their values:");
         * for (Entry<String, ExprTree> entry : attrList.entrySet()) {
         * System.out.println(entry.getKey() + ":" + entry.getValue().getKind());
         * }
         */
        ExprTree attr = attrList.get(name);
        if (attr != null) {
            return attr;
        } else if (chainedParentAd != null) {
            return chainedParentAd.lookup(name);
        } else {
            return null;
        }
    }

    public ExprTree lookupInScope(AMutableCharArrayString name, ClassAd finalScope) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        ExprTreeHolder tree = objectPool.mutableExprPool.get();
        int rval;
        state.setScopes(this);
        rval = lookupInScope(name.toString(), tree, state);
        if (rval == EvalResult.EVAL_OK.ordinal()) {
            finalScope.setValue(state.getCurAd());
            return (tree);
        }
        finalScope.setValue(null);
        return null;
    }

    public int lookupInScope(String name, ExprTreeHolder expr, EvalState state) throws HyracksDataException {

        ClassAd current = this;
        ClassAd superScope = objectPool.classAdPool.get();
        expr.setInnerTree(null);

        while (expr.getInnerTree() == null && current != null) {
            // lookups/eval's being done in the 'current' ad
            state.getCurAd().setValue(current);

            // lookup in current scope
            expr.setInnerTree(current.lookup(name));
            if ((expr.getInnerTree() != null)) {
                return EvalResult.EVAL_OK.ordinal();
            }
            try {
                if (state.getRootAd() == null) {
                    return (EvalResult.EVAL_UNDEF.ordinal());
                } else if (state.getRootAd().equals(current)) {
                    superScope = null;
                } else {
                    superScope = current.parentScope;
                }
            } catch (Throwable th) {
                th.printStackTrace();
                throw th;
            }
            if (!getSpecialAttrNames().contains(name)) {
                // continue searching from the superScope ...
                current = superScope;
                if (current == this) { // NAC - simple loop checker
                    return (EvalResult.EVAL_UNDEF.ordinal());
                }
            } else if (name.equalsIgnoreCase(ATTR_TOPLEVEL) || name.equalsIgnoreCase(ATTR_ROOT)) {
                // if the "toplevel" attribute was requested ...
                expr.setInnerTree(state.getRootAd());
                if (expr.getInnerTree() == null) { // NAC - circularity so no root
                    return EvalResult.EVAL_FAIL.ordinal(); // NAC
                } // NAC
                return (expr.getInnerTree() != null ? EvalResult.EVAL_OK.ordinal() : EvalResult.EVAL_UNDEF.ordinal());
            } else if (name.equalsIgnoreCase(ATTR_SELF) || name.equalsIgnoreCase(ATTR_MY)) {
                // if the "self" ad was requested
                expr.setInnerTree(state.getCurAd());
                return (expr.getInnerTree() != null ? EvalResult.EVAL_OK.ordinal() : EvalResult.EVAL_UNDEF.ordinal());
            } else if (name.equalsIgnoreCase(ATTR_PARENT)) {
                // the lexical parent
                expr.setInnerTree(superScope);
                return (expr.getInnerTree() != null ? EvalResult.EVAL_OK.ordinal() : EvalResult.EVAL_UNDEF.ordinal());
            } else if (name.equalsIgnoreCase(ATTR_CURRENT_TIME)) {
                // an alias for time() from old ClassAds
                expr.setInnerTree(getCurrentTimeExpr());
                return (expr.getInnerTree() != null ? EvalResult.EVAL_OK.ordinal() : EvalResult.EVAL_UNDEF.ordinal());
            }
        }
        return (EvalResult.EVAL_UNDEF.ordinal());
    }

    // --- end lookup methods

    // --- begin deletion methods
    public boolean delete(String name) throws HyracksDataException {
        CaseInsensitiveString aString = objectPool.caseInsensitiveStringPool.get();
        aString.set(name);
        boolean success = delete(aString);
        return success;
    }

    public boolean delete(CaseInsensitiveString name) throws HyracksDataException {
        boolean deleted_attribute;
        deleted_attribute = false;
        if (attrList.containsKey(name)) {
            attrList.remove(name);
            deleted_attribute = true;
        }
        // If the attribute is in the chained parent, we delete define it
        // here as undefined, whether or not it was defined here.  This is
        // behavior copied from old ClassAds. It's also one reason you
        // probably don't want to use this feature in the future.
        if (chainedParentAd != null && chainedParentAd.lookup(name) != null) {
            Value undefined_value = objectPool.valuePool.get();
            undefined_value.setUndefinedValue();
            deleted_attribute = true;
            ExprTree plit = Literal.createLiteral(undefined_value, objectPool);
            insert(name.get(), plit);
        }
        return deleted_attribute;
    }

    public boolean deepDelete(ExprTree scopeExpr, String name) throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (false);
        }
        CaseInsensitiveString aString = objectPool.caseInsensitiveStringPool.get();
        aString.set(name);
        boolean success = ad.delete(aString);
        return success;
    }

    // --- end deletion methods

    // --- begin removal methods
    public ExprTree remove(String name) throws HyracksDataException {
        ExprTree tree = null;
        if (attrList.containsKey(name)) {
            tree = attrList.remove(name);
        }

        // If the attribute is in the chained parent, we delete define it
        // here as undefined, whether or not it was defined here.  This is
        // behavior copied from old ClassAds. It's also one reason you
        // probably don't want to use this feature in the future.
        if (chainedParentAd != null && chainedParentAd.lookup(name) != null) {
            if (tree == null) {
                tree = chainedParentAd.lookup(name);
            }
            Value undefined_value = objectPool.valuePool.get();
            undefined_value.setUndefinedValue();
            ExprTree plit = Literal.createLiteral(undefined_value, objectPool);
            //why??
            insert(name, plit);
        }
        return tree;
    }

    public ExprTree deepRemove(ExprTree scopeExpr, String name) throws HyracksDataException {
        ClassAd ad = privateGetDeepScope(scopeExpr);
        if (ad == null) {
            return (null);
        }
        return (ad.remove(name));
    }

    // --- end removal methods

    @Override
    public void privateSetParentScope(ClassAd ad) {
        // already set by base class for this node; we shouldn't propagate
        // the call to sub-expressions because this is a new scope
    }

    public void modify(ClassAd mod) throws HyracksDataException {
        ClassAd ctx;
        ExprTree expr;
        Value val = objectPool.valuePool.get();

        // Step 0:  Determine Context
        if ((expr = mod.lookup(Common.ATTR_CONTEXT)) != null) {
            if ((ctx = privateGetDeepScope(expr)) == null) {
                return;
            }
        } else {
            ctx = this;
        }

        // Step 1:  Process Replace attribute
        if ((expr = mod.lookup(Common.ATTR_REPLACE)) != null) {
            ClassAd ad = objectPool.classAdPool.get();
            if (expr.publicEvaluate(val) && val.isClassAdValue(ad)) {
                ctx.clear();
                ctx.update(ad);
            }
        }

        // Step 2:  Process Updates attribute
        if ((expr = mod.lookup(Common.ATTR_UPDATES)) != null) {
            ClassAd ad = objectPool.classAdPool.get();
            if (expr.publicEvaluate(val) && val.isClassAdValue(ad)) {
                ctx.update(ad);
            }
        }

        // Step 3:  Process Deletes attribute
        if ((expr = mod.lookup(Common.ATTR_DELETES)) != null) {
            ExprList list = objectPool.exprListPool.get();
            AMutableCharArrayString attrName = objectPool.strPool.get();

            // make a first pass to check that it is a list of strings ...
            if (!expr.publicEvaluate(val) || !val.isListValue(list)) {
                return;
            }
            for (ExprTree aExpr : list.getExprList()) {
                if (!aExpr.publicEvaluate(val) || !val.isStringValue(attrName)) {
                    return;
                }
            }
            // now go through and delete all the named attributes ...
            for (ExprTree aExpr : list.getExprList()) {
                if (aExpr.publicEvaluate(val) && val.isStringValue(attrName)) {
                    ctx.delete(attrName.toString());
                }
            }
        }
    }

    @Override
    public ExprTree copy() throws HyracksDataException {
        ClassAd newAd = objectPool.classAdPool.get();
        newAd.parentScope = (parentScope == null) ? null : (ClassAd) parentScope.copy();
        newAd.chainedParentAd = chainedParentAd == null ? null : (ClassAd) chainedParentAd.copy();

        for (Entry<CaseInsensitiveString, ExprTree> entry : attrList.entrySet()) {
            newAd.insert(entry.getKey().get(), entry.getValue(), false);
        }
        return newAd;
    }

    @Override
    public boolean publicEvaluate(EvalState state, Value val) throws HyracksDataException {
        val.setClassAdValue(this);
        return (true);
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val, ExprTreeHolder tree) throws HyracksDataException {
        val.setClassAdValue(this);
        tree.setInnerTree(copy());
        return true;
    }

    @Override
    public boolean privateFlatten(EvalState state, Value val, ExprTreeHolder tree, AMutableInt32 i)
            throws HyracksDataException {
        ClassAd newAd = objectPool.classAdPool.get();
        Value eval = objectPool.valuePool.get();
        ExprTreeHolder etree = objectPool.mutableExprPool.get();
        ClassAd oldAd;

        tree.setInnerTree(null); // Just to be safe...  wenger 2003-12-11.

        oldAd = state.getCurAd();
        state.setCurAd(this);

        for (Entry<CaseInsensitiveString, ExprTree> entry : attrList.entrySet()) {
            // flatten expression
            if (!entry.getValue().publicFlatten(state, eval, etree)) {
                tree.setInnerTree(null);
                eval.setUndefinedValue();
                state.setCurAd(oldAd);
                return false;
            }

            // if a value was obtained, convert it to a literal
            if (etree.getInnerTree() == null) {
                etree.setInnerTree(Literal.createLiteral(eval, objectPool));
                if (etree.getInnerTree() == null) {
                    tree.setInnerTree(null);
                    eval.setUndefinedValue();
                    state.setCurAd(oldAd);
                    return false;
                }
            }
            CaseInsensitiveString key = objectPool.caseInsensitiveStringPool.get();
            ExprTreeHolder value = objectPool.mutableExprPool.get();
            key.set(entry.getKey().get());
            value.copyFrom(etree);
            newAd.attrList.put(key, value);
            eval.setUndefinedValue();
        }

        tree.setInnerTree(newAd);
        state.setCurAd(oldAd);
        return true;
    }

    public boolean evaluateAttr(String attr, Value val) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        ExprTreeHolder tree = objectPool.mutableExprPool.get();
        state.setScopes(this);
        switch (lookupInScope(attr, tree, state)) {
            case ExprTree.EVAL_FAIL_Int:
                return false;
            case ExprTree.EVAL_OK_Int:
                return (tree.publicEvaluate(state, val));
            case ExprTree.EVAL_UNDEF_Int:
                val.setUndefinedValue();
                return (true);
            case ExprTree.EVAL_ERROR_Int:
                val.setErrorValue();
                return (true);
            default:
                return false;
        }
    }

    public boolean evaluateExpr(String buf, Value result) throws HyracksDataException {
        boolean successfully_evaluated;
        ExprTreeHolder tree = objectPool.mutableExprPool.get();
        ClassAdParser parser = objectPool.classAdParserPool.get();

        try {
            if (parser.parseExpression(buf, tree)) {
                successfully_evaluated = evaluateExpr(tree, result);
            } else {
                successfully_evaluated = false;
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        return successfully_evaluated;
    }

    public boolean evaluateExpr(ExprTreeHolder tree, Value val) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        state.setScopes(this);
        return (tree.publicEvaluate(state, val));
    }

    public boolean evaluateExpr(ExprTreeHolder tree, Value val, ExprTreeHolder sig) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        state.setScopes(this);
        return (tree.publicEvaluate(state, val, sig));
    }

    public boolean evaluateAttrInt(String attr, AMutableInt64 i) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        return (evaluateAttr(attr, val) && val.isIntegerValue(i));
    }

    public boolean evaluateAttrReal(String attr, AMutableDouble r) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        return (evaluateAttr(attr, val) && val.isRealValue(r));
    }

    public boolean evaluateAttrNumber(String attr, AMutableInt64 i) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        return (evaluateAttr(attr, val) && val.isNumber(i));
    }

    public boolean evaluateAttrNumber(String attr, AMutableDouble r) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        return (evaluateAttr(attr, val) && val.isNumber(r));
    }

    public boolean evaluateAttrString(String attr, AMutableCharArrayString buf, int len) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        return (evaluateAttr(attr, val) && val.isStringValue(buf, len));
    }

    public boolean evaluateAttrString(String attr, AMutableCharArrayString buf) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        return (evaluateAttr(attr, val) && val.isStringValue(buf));
    }

    public boolean evaluateAttrBool(String attr, MutableBoolean b) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        return (evaluateAttr(attr, val) && val.isBooleanValue(b));
    }

    public boolean evaluateAttrBoolEquiv(String attr, MutableBoolean b) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        return (evaluateAttr(attr, val) && val.isBooleanValueEquiv(b));
    }

    /*
     * Reference is an ordered set of Strings <The ordering uses less than ignore case>. Example
     * below
     * TreeSet<String> references = new TreeSet<String>(
     * new Comparator<String>(){
     * public int compare(String o1, String o2) {
     * return o1.compareToIgnoreCase(o2);
     * }
     * });
     *
     * // PortReferences is a Map<ClassAd,OrderedSet<Strings>>
     */

    public boolean getExternalReferences(ExprTree tree, TreeSet<String> refs, boolean fullNames)
            throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        // Treat this ad as the root of the tree for reference tracking.
        // If an attribute is only present in a parent scope of this ad,
        // then we want to treat it as an external reference.
        state.setRootAd(this);
        state.setCurAd(this);
        return (privateGetExternalReferences(tree, this, state, refs, fullNames));
    }

    public boolean privateGetExternalReferences(ExprTree expr, ClassAd ad, EvalState state, TreeSet<String> refs,
            boolean fullNames) throws HyracksDataException {
        if (expr.isTreeHolder()) {
            expr = ((ExprTreeHolder) expr).getInnerTree();
        }
        switch (expr.getKind()) {
            case LITERAL_NODE:
                // no external references here
                return (true);

            case ATTRREF_NODE: {
                ClassAd start = objectPool.classAdPool.get();
                ExprTreeHolder tree = objectPool.mutableExprPool.get();
                ExprTreeHolder result = objectPool.mutableExprPool.get();
                AMutableCharArrayString attr = objectPool.strPool.get();
                Value val = objectPool.valuePool.get();
                MutableBoolean abs = objectPool.boolPool.get();

                ((AttributeReference) expr).getComponents(tree, attr, abs);
                // establish starting point for attribute search
                if (tree.getInnerTree() == null) {
                    start = abs.booleanValue() ? state.getRootAd() : state.getCurAd();
                    if (abs.booleanValue() && (start == null)) {// NAC - circularity so no root
                        return false; // NAC
                    } // NAC
                } else {
                    if (!tree.publicEvaluate(state, val)) {
                        return (false);
                    }
                    // if the tree evals to undefined, the external references
                    // are in the tree part
                    if (val.isUndefinedValue()) {
                        if (fullNames) {
                            AMutableCharArrayString fullName = objectPool.strPool.get();
                            if (tree.getInnerTree() != null) {
                                ClassAdUnParser unparser = objectPool.prettyPrintPool.get();
                                unparser.unparse(fullName, tree);
                                fullName.appendChar('.');
                            }
                            fullName.appendString(attr);
                            refs.add(fullName.toString());
                            return true;
                        } else {
                            if (state.getDepthRemaining() <= 0) {
                                return false;
                            }
                            state.decrementDepth();
                            boolean ret = privateGetExternalReferences(tree, ad, state, refs, fullNames);
                            state.incrementDepth();
                            return ret;
                        }
                    }
                    // otherwise, if the tree didn't evaluate to a classad,
                    // we have a problem
                    if (!val.isClassAdValue(start)) {
                        return (false);
                    }
                }
                // lookup for attribute
                ClassAd curAd = state.getCurAd();
                switch (start.lookupInScope(attr.toString(), result, state)) {
                    case EVAL_ERROR_Int:
                        // some error
                        return (false);
                    case EVAL_UNDEF_Int:
                        // attr is external
                        refs.add(attr.toString());
                        state.setCurAd(curAd);
                        return (true);
                    case EVAL_OK_Int: {
                        // attr is internal; find external refs in result
                        if (state.getDepthRemaining() <= 0) {
                            state.setCurAd(curAd);
                            return false;
                        }
                        state.decrementDepth();
                        boolean rval = privateGetExternalReferences(result, ad, state, refs, fullNames);
                        state.incrementDepth();
                        state.setCurAd(curAd);
                        return (rval);
                    }

                    case EVAL_FAIL_Int:
                    default:
                        // enh??
                        return (false);
                }
            }
            case OP_NODE: {
                // recurse on subtrees
                AMutableInt32 opKind = objectPool.int32Pool.get();
                ExprTreeHolder t1 = objectPool.mutableExprPool.get();
                ExprTreeHolder t2 = objectPool.mutableExprPool.get();
                ExprTreeHolder t3 = objectPool.mutableExprPool.get();

                ((Operation) expr).getComponents(opKind, t1, t2, t3);
                if (t1.getInnerTree() != null && !privateGetExternalReferences(t1, ad, state, refs, fullNames)) {
                    return (false);
                }
                if (t2.getInnerTree() != null && !privateGetExternalReferences(t2, ad, state, refs, fullNames)) {
                    return (false);
                }
                if (t3.getInnerTree() != null && !privateGetExternalReferences(t3, ad, state, refs, fullNames)) {
                    return (false);
                }
                return (true);
            }
            case FN_CALL_NODE: {
                // recurse on subtrees
                AMutableCharArrayString fnName = objectPool.strPool.get();
                ExprList args = objectPool.exprListPool.get();
                ((FunctionCall) expr).getComponents(fnName, args);
                for (ExprTree tree : args.getExprList()) {
                    if (!privateGetExternalReferences(tree, ad, state, refs, fullNames)) {
                        return (false);
                    }
                }
                return (true);
            }
            case CLASSAD_NODE: {
                // recurse on subtrees
                Map<CaseInsensitiveString, ExprTree> attrs = objectPool.strToExprPool.get();
                ((ClassAd) expr).getComponents(attrs, objectPool);
                for (Entry<CaseInsensitiveString, ExprTree> entry : attrs.entrySet()) {
                    if (state.getDepthRemaining() <= 0) {
                        return false;
                    }
                    state.decrementDepth();
                    boolean ret = privateGetExternalReferences(entry.getValue(), ad, state, refs, fullNames);
                    state.incrementDepth();
                    if (!ret) {
                        return (false);
                    }
                }
                return (true);
            }
            case EXPR_LIST_NODE: {
                // recurse on subtrees
                ExprList exprs = objectPool.exprListPool.get();

                ((ExprList) expr).getComponents(exprs);
                for (ExprTree exprTree : exprs.getExprList()) {
                    if (state.getDepthRemaining() <= 0) {
                        return false;
                    }
                    state.decrementDepth();

                    boolean ret = privateGetExternalReferences(exprTree, ad, state, refs, fullNames);

                    state.incrementDepth();
                    if (!ret) {
                        return (false);
                    }
                }
                return (true);
            }
            default:
                return false;
        }
    }

    // PortReferences is a Map<ClassAd,TreeSet<Strings>>
    public boolean getExternalReferences(ExprTree tree, Map<ClassAd, TreeSet<String>> refs)
            throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();
        // Treat this ad as the root of the tree for reference tracking.
        // If an attribute is only present in a parent scope of this ad,
        // then we want to treat it as an external reference.
        state.setRootAd(this);
        state.setCurAd(this);

        return (privateGetExternalReferences(tree, this, state, refs));
    }

    public boolean privateGetExternalReferences(ExprTree expr, ClassAd ad, EvalState state,
            Map<ClassAd, TreeSet<String>> refs) throws HyracksDataException {
        switch (expr.getKind()) {
            case LITERAL_NODE:
                // no external references here
                return (true);

            case ATTRREF_NODE: {
                ClassAd start = objectPool.classAdPool.get();
                ExprTreeHolder tree = objectPool.mutableExprPool.get();
                ExprTreeHolder result = objectPool.mutableExprPool.get();
                AMutableCharArrayString attr = objectPool.strPool.get();
                Value val = objectPool.valuePool.get();
                MutableBoolean abs = objectPool.boolPool.get();

                ((AttributeReference) expr).getComponents(tree, attr, abs);
                // establish starting point for attribute search
                if (tree.getInnerTree() == null) {
                    start = abs.booleanValue() ? state.getRootAd() : state.getCurAd();
                    if (abs.booleanValue() && (start == null)) {// NAC - circularity so no root
                        return false; // NAC
                    } // NAC
                } else {
                    if (!tree.publicEvaluate(state, val)) {
                        return (false);
                    }
                    // if the tree evals to undefined, the external references
                    // are in the tree part
                    if (val.isUndefinedValue()) {
                        return (privateGetExternalReferences(tree, ad, state, refs));
                    }
                    // otherwise, if the tree didn't evaluate to a classad,
                    // we have a problem
                    if (!val.isClassAdValue(start)) {
                        return (false);
                    }

                    // make sure that we are starting from a "valid" scope
                    if (!refs.containsKey(start) && start != this) {
                        return (false);
                    }
                }
                // lookup for attribute
                ClassAd curAd = state.getCurAd();
                TreeSet<String> pitr = refs.get(start);
                if (pitr == null) {
                    pitr = objectPool.strSetPool.get();
                    refs.put(start, pitr);
                }
                switch (start.lookupInScope(attr.toString(), result, state)) {
                    case EVAL_ERROR_Int:
                        // some error
                        return (false);

                    case EVAL_UNDEF_Int:
                        // attr is external
                        pitr.add(attr.toString());
                        state.setCurAd(curAd);
                        return (true);
                    case EVAL_OK_Int: {
                        // attr is internal; find external refs in result
                        boolean rval = privateGetExternalReferences(result, ad, state, refs);
                        state.setCurAd(curAd);
                        return (rval);
                    }

                    case EVAL_FAIL_Int:
                    default:
                        // enh??
                        return (false);
                }
            }

            case OP_NODE: {
                // recurse on subtrees
                AMutableInt32 opKind = objectPool.int32Pool.get();
                ExprTreeHolder t1 = objectPool.mutableExprPool.get();
                ExprTreeHolder t2 = objectPool.mutableExprPool.get();
                ExprTreeHolder t3 = objectPool.mutableExprPool.get();
                ((Operation) expr).getComponents(opKind, t1, t2, t3);
                if (t1.getInnerTree() != null && !privateGetExternalReferences(t1, ad, state, refs)) {
                    return (false);
                }
                if (t2.getInnerTree() != null && !privateGetExternalReferences(t2, ad, state, refs)) {
                    return (false);
                }
                if (t3.getInnerTree() != null && !privateGetExternalReferences(t3, ad, state, refs)) {
                    return (false);
                }
                return (true);
            }

            case FN_CALL_NODE: {
                // recurse on subtrees
                AMutableCharArrayString fnName = objectPool.strPool.get();
                ExprList args = objectPool.exprListPool.get();

                ((FunctionCall) expr).getComponents(fnName, args);
                for (ExprTree exprTree : args.getExprList()) {
                    if (!privateGetExternalReferences(exprTree, ad, state, refs)) {
                        return (false);
                    }
                }
                return (true);
            }

            case CLASSAD_NODE: {
                // recurse on subtrees
                HashMap<CaseInsensitiveString, ExprTree> attrs = objectPool.strToExprPool.get();

                ((ClassAd) expr).getComponents(attrs, objectPool);
                for (Entry<CaseInsensitiveString, ExprTree> entry : attrs.entrySet()) {
                    if (!privateGetExternalReferences(entry.getValue(), ad, state, refs)) {
                        return (false);
                    }
                }
                return (true);
            }

            case EXPR_LIST_NODE: {
                // recurse on subtrees
                ExprList exprs = objectPool.exprListPool.get();
                ((ExprList) expr).getComponents(exprs);
                for (ExprTree exprTree : exprs.getExprList()) {
                    if (!privateGetExternalReferences(exprTree, ad, state, refs)) {
                        return (false);
                    }
                }
                return (true);
            }

            default:
                return false;
        }
    }

    /*
     * Reference is an ordered set of Strings <The ordering uses less than ignore case>. Example
     * below
     * TreeSet<String> references = new TreeSet<String>(
     * new Comparator<String>(){
     * public int compare(String o1, String o2) {
     * return o1.compareToIgnoreCase(o2);
     * }
     * });
     *
     * // PortReferences is a Map<ClassAd,OrderedSet<Strings>>
     */
    public boolean getInternalReferences(ExprTree tree, TreeSet<String> refs, boolean fullNames)
            throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();

        // Treat this ad as the root of the tree for reference tracking.
        // If an attribute is only present in a parent scope of this ad,
        // then we want to treat it as an external reference.
        state.setRootAd(this);
        state.setCurAd(this);

        return (privateGetInternalReferences(tree, this, state, refs, fullNames));
    }

    //this is closely modelled off of _GetExternalReferences in the new_classads.
    public boolean privateGetInternalReferences(ExprTree expr, ClassAd ad, EvalState state, TreeSet<String> refs,
            boolean fullNames) throws HyracksDataException {

        switch (expr.getKind()) {
            //nothing to be found here!
            case LITERAL_NODE: {
                return true;
            }

            case ATTRREF_NODE: {
                ClassAd start = objectPool.classAdPool.get();
                ExprTreeHolder tree = objectPool.mutableExprPool.get();
                ExprTreeHolder result = objectPool.mutableExprPool.get();
                AMutableCharArrayString attr = objectPool.strPool.get();
                Value val = objectPool.valuePool.get();
                MutableBoolean abs = objectPool.boolPool.get();

                ((AttributeReference) expr).getComponents(tree, attr, abs);

                //figuring out which state to base this off of
                if (tree.getInnerTree() == null) {
                    start = abs.booleanValue() ? state.getRootAd() : state.getCurAd();
                    //remove circularity
                    if (abs.booleanValue() && (start == null)) {
                        return false;
                    }
                } else {
                    boolean orig_inAttrRefScope = state.isInAttrRefScope();
                    state.setInAttrRefScope(true);
                    boolean rv = privateGetInternalReferences(tree, ad, state, refs, fullNames);
                    state.setInAttrRefScope(orig_inAttrRefScope);
                    if (!rv) {
                        return false;
                    }

                    if (!tree.publicEvaluate(state, val)) {
                        return false;
                    }

                    // TODO Do we need extra handling for list values?
                    //   Should types other than undefined, error, or list
                    //   cause a failure?
                    if (val.isUndefinedValue()) {
                        return true;
                    }

                    //otherwise, if the tree didn't evaluate to a classad,
                    //we have a problemo, mon.
                    //TODO: but why?
                    if (!val.isClassAdValue(start)) {
                        return false;
                    }
                }

                ClassAd curAd = state.getCurAd();
                switch (start.lookupInScope(attr.toString(), result, state)) {
                    case EVAL_ERROR_Int:
                        return false;
                    //attr is external, so let's find the internals in that
                    //result
                    //JUST KIDDING
                    case EVAL_UNDEF_Int: {

                        //boolean rval = _GetInternalReferences(result, ad, state, refs, fullNames);
                        //state.getCurAd() = curAd;
                        return true;
                    }

                    case EVAL_OK_Int: {
                        //whoo, it's internal.
                        // Check whether the attribute was found in the root
                        // ad for this evaluation and that the attribute isn't
                        // one of our special ones (self, parent, my, etc.).
                        // If the ad actually has an attribute with the same
                        // name as one of our special attributes, then count
                        // that as an internal reference.
                        // TODO LookupInScope() knows whether it's returning
                        //   the expression of one of the special attributes
                        //   or that of an attribute that actually appears in
                        //   the ad. If it told us which one, then we could
                        //   avoid the Lookup() call below.
                        if (state.getCurAd() == state.getRootAd() && state.getCurAd().lookup(attr.toString()) != null) {
                            refs.add(attr.toString());
                        }
                        if (state.getDepthRemaining() <= 0) {
                            state.setCurAd(curAd);
                            return false;
                        }
                        state.decrementDepth();

                        boolean rval = privateGetInternalReferences(result, ad, state, refs, fullNames);

                        state.incrementDepth();
                        //TODO: Does this actually matter?
                        state.setCurAd(curAd);
                        return rval;
                    }

                    case EVAL_FAIL_Int:
                    default:
                        // "enh??"
                        return false;
                }
            }

            case OP_NODE: {

                //recurse on subtrees
                AMutableInt32 op = objectPool.int32Pool.get();
                ExprTreeHolder t1 = objectPool.mutableExprPool.get();
                ExprTreeHolder t2 = objectPool.mutableExprPool.get();
                ExprTreeHolder t3 = objectPool.mutableExprPool.get();
                ((Operation) expr).getComponents(op, t1, t2, t3);
                if (t1.getInnerTree() != null && !privateGetInternalReferences(t1, ad, state, refs, fullNames)) {
                    return false;
                }

                if (t2.getInnerTree() != null && !privateGetInternalReferences(t2, ad, state, refs, fullNames)) {
                    return false;
                }

                if (t3.getInnerTree() != null && !privateGetInternalReferences(t3, ad, state, refs, fullNames)) {
                    return false;
                }
                return true;
            }

            case FN_CALL_NODE: {
                //recurse on the subtrees!
                AMutableCharArrayString fnName = objectPool.strPool.get();
                ExprList args = objectPool.exprListPool.get();

                ((FunctionCall) expr).getComponents(fnName, args);
                for (ExprTree exprTree : args.getExprList()) {
                    if (!privateGetInternalReferences(exprTree, ad, state, refs, fullNames)) {
                        return false;
                    }
                }

                return true;
            }

            case CLASSAD_NODE: {
                //also recurse on subtrees...
                HashMap<CaseInsensitiveString, ExprTree> attrs = objectPool.strToExprPool.get();

                // If this ClassAd is only being used here as the scoping
                // for an attribute reference, don't recurse into all of
                // its attributes.
                if (state.isInAttrRefScope()) {
                    return true;
                }

                ((ClassAd) expr).getComponents(attrs, objectPool);
                for (Entry<CaseInsensitiveString, ExprTree> entry : attrs.entrySet()) {
                    if (state.getDepthRemaining() <= 0) {
                        return false;
                    }
                    state.decrementDepth();

                    boolean ret = privateGetInternalReferences(entry.getValue(), ad, state, refs, fullNames);

                    state.incrementDepth();
                    if (!ret) {
                        return false;
                    }
                }

                return true;
            }

            case EXPR_LIST_NODE: {
                ExprList exprs = objectPool.exprListPool.get();

                ((ExprList) expr).getComponents(exprs);
                for (ExprTree exprTree : exprs.getExprList()) {
                    if (state.getDepthRemaining() <= 0) {
                        return false;
                    }
                    state.decrementDepth();

                    boolean ret = privateGetInternalReferences(exprTree, ad, state, refs, fullNames);

                    state.incrementDepth();
                    if (!ret) {
                        return false;
                    }
                }

                return true;
            }

            default:
                return false;

        }
    }

    public boolean publicFlatten(ExprTree tree, Value val, ExprTreeHolder fexpr) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();

        state.setScopes(this);
        return (tree.publicFlatten(state, val, fexpr));
    }

    public boolean flattenAndInline(ExprTree tree, Value val, ExprTreeHolder fexpr) throws HyracksDataException {
        EvalState state = objectPool.evalStatePool.get();

        state.setScopes(this);
        state.setFlattenAndInline(true);
        return (tree.publicFlatten(state, val, fexpr));
    }

    public void chainToAd(ClassAd new_chain_parent_ad) {
        if (new_chain_parent_ad != null) {
            chainedParentAd = new_chain_parent_ad;
        }
    }

    public int pruneChildAd() {
        int iRet = 0;

        if (chainedParentAd != null) {
            // loop through cleaning all expressions which are the same.
            Iterator<Entry<CaseInsensitiveString, ExprTree>> it = attrList.entrySet().iterator();
            while (it.hasNext()) {
                Entry<CaseInsensitiveString, ExprTree> entry = it.next();
                ExprTree tree = chainedParentAd.lookup(entry.getKey());

                if (tree != null && tree.sameAs(entry.getValue())) {
                    // 1st remove from dirty list
                    it.remove();
                    iRet++;
                }
            }
        }

        return iRet;
    }

    public ClassAd getChainedParentAd() {
        return chainedParentAd;
    }

    public void setValue(ClassAd value) throws HyracksDataException {
        copyFrom(value);
    }

    @Override
    public int size() {
        return attrList.size();
    }

    public static void valStr(AMutableCharArrayString szUnparsedValue, ExprTree pTree) {
        szUnparsedValue.appendString(pTree.toString());
    }

    public static void valStr(AMutableCharArrayString szOut, boolean tValue) {
        szOut.appendString(tValue ? "true" : "false");
    }

    @Override
    public NodeKind getKind() {
        return NodeKind.CLASSAD_NODE;
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val) throws HyracksDataException {
        val.setClassAdValue(this);
        return (true);
    }

    public void insertAttr(String name, double value) throws HyracksDataException {
        insertAttr(name, value, NumberFactor.NO_FACTOR);
    }

    public void createParser() {
        parser = objectPool.classAdParserPool.get();
    }
}
