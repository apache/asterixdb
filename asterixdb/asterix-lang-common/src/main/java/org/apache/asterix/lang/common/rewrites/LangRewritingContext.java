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
package org.apache.asterix.lang.common.rewrites;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.hyracks.algebricks.core.algebra.base.Counter;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public final class LangRewritingContext {
    private final IWarningCollector warningCollector;
    private Counter varCounter;
    private int systemVarCounter = 1;
    private Map<Integer, VarIdentifier> oldVarIdToNewVarId = new HashMap<>();

    public LangRewritingContext(int varCounter, IWarningCollector warningCollector) {
        this.varCounter = new Counter(varCounter);
        this.warningCollector = warningCollector;
    }

    public Counter getVarCounter() {
        return varCounter;
    }

    /**
     * Generate a new variable with the same identifier (varValue) but a different Id.
     *
     * @param oldId
     *            , the old variable id
     * @param varValue
     *            , the identifier
     * @return the new varible.
     */
    public VarIdentifier mapOldId(Integer oldId, String varValue) {
        int n = increaseAndGetCtr();
        VarIdentifier newVar = new VarIdentifier(varValue);
        newVar.setId(n);
        oldVarIdToNewVarId.put(oldId, newVar);
        return newVar;
    }

    public VarIdentifier getRewrittenVar(Integer oldId) {
        return oldVarIdToNewVarId.get(oldId);
    }

    public VarIdentifier newVariable() {
        int id = increaseAndGetCtr();
        // Prefixes system-generated variables with "#".
        return new VarIdentifier("#" + (systemVarCounter++), id);
    }

    private int increaseAndGetCtr() {
        varCounter.inc();
        return varCounter.get();
    }

    public IWarningCollector getWarningCollector() {
        return warningCollector;
    }
}
