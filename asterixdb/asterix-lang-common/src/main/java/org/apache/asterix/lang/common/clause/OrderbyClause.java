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
package org.apache.asterix.lang.common.clause;

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Clause;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;

public class OrderbyClause implements Clause {
    private List<Expression> orderbyList;
    private List<OrderModifier> modifierList;
    private IRangeMap rangeMap;
    private int numFrames = -1;
    private int numTuples = -1;

    public OrderbyClause() {
    }

    public OrderbyClause(List<Expression> orderbyList, List<OrderModifier> modifierList) {
        this.orderbyList = orderbyList;
        this.modifierList = modifierList;
    }

    public List<Expression> getOrderbyList() {
        return orderbyList;
    }

    public void setOrderbyList(List<Expression> orderbyList) {
        this.orderbyList = orderbyList;
    }

    public List<OrderModifier> getModifierList() {
        return modifierList;
    }

    public void setModifierList(List<OrderModifier> modifierList) {
        this.modifierList = modifierList;
    }

    @Override
    public ClauseType getClauseType() {
        return ClauseType.ORDER_BY_CLAUSE;
    }

    public enum OrderModifier {
        ASC,
        DESC
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    public int getNumFrames() {
        return numFrames;
    }

    public void setNumFrames(int numFrames) {
        this.numFrames = numFrames;
    }

    public int getNumTuples() {
        return numTuples;
    }

    public void setNumTuples(int numTuples) {
        this.numTuples = numTuples;
    }

    public IRangeMap getRangeMap() {
        return rangeMap;
    }

    public void setRangeMap(IRangeMap rangeMap) {
        this.rangeMap = rangeMap;
    }
}
