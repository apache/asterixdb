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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ExprList extends ExprTree {

    private final List<ExprTree> exprList;
    private final EvalState state;
    public boolean isShared = false;

    public ExprList(List<ExprTree> exprs, ClassAdObjectPool objectPool) {
        super(objectPool);
        exprList = new ArrayList<ExprTree>();
        this.state = new EvalState(this.objectPool);
        copyList(exprs);
        return;
    }

    public ExprList(ClassAdObjectPool objectPool) {
        super(objectPool);
        this.state = new EvalState(this.objectPool);
        this.exprList = new ArrayList<ExprTree>();
    }

    public ExprList(boolean b, ClassAdObjectPool objectPool) {
        super(objectPool);
        this.state = new EvalState(this.objectPool);
        this.exprList = new ArrayList<ExprTree>();
        this.isShared = b;
    }

    public boolean copyFrom(ExprList exprList) throws HyracksDataException {
        this.exprList.clear();
        copyList(exprList.getExprList());
        this.state.set(exprList.state);
        return true;
    }

    public int getlast() {
        return exprList == null ? 0 : exprList.size() - 1;
    }

    public ExprTree get(int i) {
        return exprList.get(i);
    }

    @Override
    public int size() {
        return exprList == null ? 0 : exprList.size();
    }

    // called from FunctionCall
    @Override
    public void privateSetParentScope(ClassAd scope) {
        for (ExprTree tree : exprList) {
            tree.setParentScope(scope);
        }
    }

    @Override
    public NodeKind getKind() {
        return NodeKind.EXPR_LIST_NODE;
    }

    public List<ExprTree> getExprList() {
        return exprList;
    }

    public Iterator<ExprTree> iterator() {
        return exprList.iterator();
    }

    public void setValue(ExprList value) throws HyracksDataException {
        if (value == null) {
            clear();
        } else {
            copyFrom(value);
        }
    }

    public void add(ExprTree expr) {
        exprList.add(expr.self());
    }

    public void setExprList(List<ExprTree> exprList) {
        this.exprList.clear();
        this.exprList.addAll(exprList);
    }

    public void clear() {
        exprList.clear();
    }

    @Override
    public ExprTree copy() throws HyracksDataException {
        ExprList newList = objectPool.exprListPool.get();
        newList.copyFrom(this);
        return newList;
    }

    @Override
    public boolean sameAs(ExprTree tree) {
        boolean is_same;
        if (this == tree) {
            is_same = true;
        } else if (tree.getKind() != NodeKind.EXPR_LIST_NODE) {
            is_same = false;
        } else {
            ExprList other_list = (ExprList) tree.getTree();
            if (exprList.size() != other_list.size()) {
                is_same = false;
            } else {
                is_same = true;
                for (int i = 0; i < exprList.size(); i++) {
                    if (!exprList.get(i).sameAs(other_list.get(i))) {
                        is_same = false;
                        break;
                    }
                }
            }
        }
        return is_same;
    }

    public static ExprList createExprList(List<ExprTree> exprs, ClassAdObjectPool objectPool) {
        ExprList el = objectPool.exprListPool.get();
        el.copyList(exprs);
        return el;
    }

    public static ExprList createExprList(ExprList exprs, ClassAdObjectPool objectPool) {
        ExprList el = objectPool.exprListPool.get();
        el.copyList(exprs.exprList);
        return el;
    }

    public void getComponents(List<ExprTree> exprs) {
        exprs.clear();
        exprs.addAll(exprList);
    }

    public void getComponents(ExprList list) throws HyracksDataException {
        list.clear();
        list.addAll(exprList);
        /*
        for(ExprTree e: exprList){
            list.add(e.Copy());
        }*/
    }

    private void addAll(List<ExprTree> exprList) {
        this.exprList.addAll(exprList);
    }

    public void insert(ExprTree t) {
        exprList.add(t);
    }

    public void push_back(ExprTree t) {
        exprList.add(t);
    }

    public void erase(int f, int to) {
        int listInitialSize = exprList.size();
        Iterator<ExprTree> it = exprList.iterator();
        int i = 0;
        while (i < listInitialSize && i < to) {
            it.next();
            if (i >= f) {
                it.remove();
            }
            i++;
        }
        return;
    }

    public void erase(int index) {
        exprList.remove(index);
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val) throws HyracksDataException {
        val.setListValue(this);
        return (true);
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val, ExprTreeHolder sig) throws HyracksDataException {
        val.setListValue(this);
        sig.setInnerTree(copy());
        return (sig.getInnerTree() != null);
    }

    @Override
    public boolean privateFlatten(EvalState state, Value val, ExprTreeHolder tree, AMutableInt32 aInt)
            throws HyracksDataException {
        ExprTreeHolder nexpr = objectPool.mutableExprPool.get();
        Value tempVal = objectPool.valuePool.get();
        ExprList newList = objectPool.exprListPool.get();

        tree.setInnerTree(null); // Just to be safe...  wenger 2003-12-11.

        for (ExprTree expr : exprList) {
            // flatten the constituent expression
            if (!expr.publicFlatten(state, tempVal, nexpr)) {
                return false;
            }
            // if only a value was obtained, convert to an expression
            if (nexpr.getInnerTree() == null) {
                nexpr.setInnerTree(Literal.createLiteral(tempVal, objectPool));
                if (nexpr.getInnerTree() == null) {
                    return false;
                }
            }
            // add the new expression to the flattened list
            newList.push_back(nexpr);
        }
        tree.setInnerTree(newList);
        return true;
    }

    public void copyList(List<ExprTree> exprs) {
        for (ExprTree expr : exprs) {
            exprList.add(expr);
        }
    }

    public boolean getValue(Value val, ExprTree tree, EvalState es) throws HyracksDataException {
        EvalState currentState = objectPool.evalStatePool.get();

        if (tree == null) {
            return false;
        }

        // if called from user code, es == NULL so we use &state instead
        currentState = (es != null) ? es : state;

        if (currentState.getDepthRemaining() <= 0) {
            val.setErrorValue();
            return false;
        }
        currentState.decrementDepth();

        ClassAd tmpScope = currentState.getCurAd();
        currentState.setCurAd(tree.getParentScope());
        tree.publicEvaluate(currentState, val);
        currentState.setCurAd(tmpScope);

        currentState.incrementDepth();

        return true;
    }

    @Override
    public void reset() {
        exprList.clear();
        state.reset();
    }
}
