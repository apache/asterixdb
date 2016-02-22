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
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ExprTreeHolder extends ExprTree {
    private ExprTree innerTree;

    @Override
    public ClassAd getParentScope() {
        return innerTree.parentScope;
    }

    @Override
    public void copyFrom(ExprTree tree) throws HyracksDataException {
        if (tree == null) {
            innerTree = null;
        } else {
            if (tree.isTreeHolder()) {
                tree = ((ExprTreeHolder) tree).innerTree;
            }
            if (innerTree == null) {
                innerTree = tree.copy();
            } else {
                innerTree.copyFrom(tree);
            }
        }
    }

    @Override
    public void reset() {
        this.innerTree = null;
    }

    @Override
    public void puke() throws HyracksDataException {
        PrettyPrint unp = new PrettyPrint();
        AMutableCharArrayString buffer = new AMutableCharArrayString();
        unp.unparse(buffer, innerTree);
        System.out.println(buffer.toString());
    }

    @Override
    public void resetExprTree(ExprTree expr) {
        setInnerTree(expr);
    }

    @Override
    public ExprTree getTree() {
        return innerTree;
    }

    @Override
    public ExprTree self() {
        return innerTree;
    }

    @Override
    public boolean isTreeHolder() {
        return true;
    }

    public ExprTreeHolder() {
        innerTree = null;
    }

    public ExprTreeHolder(ExprTree tree) {
        setInnerTree(tree);
    }

    public ExprTree getInnerTree() {
        return innerTree;
    }

    public void setInnerTree(ExprTree innerTree) {
        if (innerTree != null && innerTree.isTreeHolder()) {
            setInnerTree(((ExprTreeHolder) innerTree).getInnerTree());
        } else {
            this.innerTree = innerTree;
        }
    }

    @Override
    public ExprTree copy() throws HyracksDataException {
        return innerTree.copy();
    }

    @Override
    public NodeKind getKind() {
        return innerTree.getKind();
    }

    @Override
    public boolean sameAs(ExprTree tree) {
        if (tree == null) {
            return innerTree == null;
        }
        return innerTree == null ? false : innerTree.sameAs(tree);
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val) throws HyracksDataException {
        return innerTree.privateEvaluate(state, val);
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val, ExprTreeHolder tree) throws HyracksDataException {
        return innerTree.privateEvaluate(state, val, tree);
    }

    @Override
    public boolean privateFlatten(EvalState state, Value val, ExprTreeHolder tree, AMutableInt32 op)
            throws HyracksDataException {
        return innerTree.privateFlatten(state, val, tree, op);
    }

    @Override
    public int size() {
        return innerTree != null ? 1 : 0;
    }

    @Override
    protected void privateSetParentScope(ClassAd scope) {
        innerTree.privateSetParentScope(scope);
    }
}
