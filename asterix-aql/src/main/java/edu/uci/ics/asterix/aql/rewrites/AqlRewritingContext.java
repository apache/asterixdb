/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.rewrites;

import java.util.HashMap;

import edu.uci.ics.asterix.aql.expression.VarIdentifier;

public final class AqlRewritingContext {
    private int varCounter;
    private HashMap<Integer, VarIdentifier> oldVarIdToNewVarId = new HashMap<Integer, VarIdentifier>();

    public AqlRewritingContext(int varCounter) {
        this.varCounter = varCounter;
    }

    public int getVarCounter() {
        return varCounter;
    }

    public VarIdentifier mapOldId(Integer oldId, String varValue) {
        int n = newId();
        VarIdentifier newVar = new VarIdentifier(varValue);
        newVar.setId(n);
        oldVarIdToNewVarId.put(oldId, newVar);
        return newVar;
    }

    public VarIdentifier mapOldVarIdentifier(VarIdentifier vi) {
        return mapOldId(vi.getId(), vi.getValue());
    }

    public VarIdentifier getRewrittenVar(Integer oldId) {
        return oldVarIdToNewVarId.get(oldId);
    }

    public VarIdentifier newVariable() {
        int id = newId();
        return new VarIdentifier("@@" + id, id);
    }

    private int newId() {
        varCounter++;
        return varCounter;
    }
}
