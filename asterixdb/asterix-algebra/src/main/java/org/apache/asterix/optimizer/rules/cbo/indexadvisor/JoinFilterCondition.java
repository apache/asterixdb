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
package org.apache.asterix.optimizer.rules.cbo.indexadvisor;

import java.util.List;

import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class JoinFilterCondition {
    private final FunctionIdentifier fi;
    private final List<String> lhsFields;
    private final List<String> rhsFields;
    private final LogicalVariable lhsVar;
    private final LogicalVariable rhsVar;

    public JoinFilterCondition(FunctionIdentifier fi, LogicalVariable lhsVar, List<String> lhsFields,
            LogicalVariable rhsVar, List<String> rhsFields) {
        this.fi = fi;
        this.lhsFields = lhsFields;
        this.rhsFields = rhsFields;
        this.lhsVar = lhsVar;
        this.rhsVar = rhsVar;
    }

    public List<String> getLhsFields() {
        return lhsFields;
    }

    public List<String> getRhsFields() {
        return rhsFields;
    }

    public LogicalVariable getLhsVar() {
        return lhsVar;
    }

    public LogicalVariable getRhsVar() {
        return rhsVar;
    }
}
