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
package org.apache.asterix.lang.sqlpp.struct;

import java.util.Objects;

import org.apache.asterix.lang.sqlpp.optype.SetOpType;

public class SetOperationRight {

    private SetOpType opType;
    private boolean setSemantics;
    private SetOperationInput setOperationRightInput;

    public SetOperationRight(SetOpType opType, boolean setSemantics, SetOperationInput setOperationRight) {
        this.opType = opType;
        this.setSemantics = setSemantics;
        this.setOperationRightInput = setOperationRight;
    }

    public SetOpType getSetOpType() {
        return opType;
    }

    public boolean isSetSemantics() {
        return setSemantics;
    }

    public SetOperationInput getSetOperationRightInput() {
        return setOperationRightInput;
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, setOperationRightInput, setSemantics);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof SetOperationRight)) {
            return false;
        }
        SetOperationRight target = (SetOperationRight) object;
        return Objects.equals(opType, target.opType)
                && Objects.equals(setOperationRightInput, target.setOperationRightInput)
                && setSemantics == target.setSemantics;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(opType + " " + (setSemantics ? "" : " all "));
        sb.append(setOperationRightInput);
        return sb.toString();
    }
}
