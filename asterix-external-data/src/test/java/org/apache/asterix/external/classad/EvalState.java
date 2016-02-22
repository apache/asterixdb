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

public class EvalState {

    private int depthRemaining; // max recursion depth - current depth
    // Normally, rootAd will be the ClassAd at the root of the tree
    // of ExprTrees in the current evaluation. That is, the parent
    // scope whose parent scope is NULL.
    // It can be set to a closer parent scope. Then that ClassAd is
    // treated like it has no parent scope for LookupInScope() and
    // Evaluate().
    private ClassAd rootAd;
    private ClassAd curAd;
    private boolean flattenAndInline; // NAC
    private boolean inAttrRefScope;

    public boolean isInAttrRefScope() {
        return inAttrRefScope;
    }

    public void setFlattenAndInline(boolean flattenAndInline) {
        this.flattenAndInline = flattenAndInline;
    }

    public void setInAttrRefScope(boolean inAttrRefScope) {
        this.inAttrRefScope = inAttrRefScope;
    }

    public EvalState() {
        rootAd = new ClassAd();
        curAd = new ClassAd();
        depthRemaining = ExprTree.MAX_CLASSAD_RECURSION;
        flattenAndInline = false; // NAC
        inAttrRefScope = false;
    }

    public boolean isFlattenAndInline() {
        return flattenAndInline;
    }

    public void setScopes(ClassAd curScope) {
        curAd = curScope;
        setRootScope();
    }

    public void setRootScope() {
        ClassAd prevScope = curAd;
        if (curAd == null) {
            rootAd = null;
        } else {
            ClassAd curScope = curAd.getParentScope();

            while (curScope != null) {
                if (curScope == curAd) { // NAC - loop detection
                    rootAd = null;
                    return; // NAC
                } // NAC
                prevScope = curScope;
                curScope = curScope.getParentScope();
            }

            rootAd = prevScope;
        }
        return;
    }

    public void reset() {
        rootAd.reset();
        curAd.reset();
        depthRemaining = ExprTree.MAX_CLASSAD_RECURSION;
        flattenAndInline = false;
        inAttrRefScope = false;
    }

    public ClassAd getRootAd() {
        return rootAd;
    }

    public ClassAd getCurAd() {
        return curAd;
    }

    public void setCurAd(ClassAd curAd) {
        this.curAd = curAd;
    }

    public int getDepthRemaining() {
        return depthRemaining;
    }

    public void decrementDepth() {
        depthRemaining--;
    }

    public void incrementDepth() {
        depthRemaining++;
    }

    public void setRootAd(ClassAd classAd) {
        this.rootAd = classAd;
    }
}
