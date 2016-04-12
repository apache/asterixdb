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

package org.apache.asterix.lang.sqlpp.clause;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.lang.sqlpp.struct.SetOperationInput;
import org.apache.asterix.lang.sqlpp.struct.SetOperationRight;
import org.apache.asterix.lang.sqlpp.visitor.base.ISqlppVisitor;

public class SelectSetOperation implements Clause {

    private SetOperationInput leftInput;
    private List<SetOperationRight> rightInputs;

    public SelectSetOperation(SetOperationInput leftInput, List<SetOperationRight> rightInputs) {
        this.leftInput = leftInput;
        this.rightInputs = rightInputs == null ? new ArrayList<SetOperationRight>() : rightInputs;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return ((ISqlppVisitor<R, T>) visitor).visit(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.SELECT_SET_OPERATION;
    }

    public SetOperationInput getLeftInput() {
        return leftInput;
    }

    public List<SetOperationRight> getRightInputs() {
        return rightInputs;
    }

    public boolean hasRightInputs() {
        return rightInputs != null && rightInputs.size() > 0;
    }

}
