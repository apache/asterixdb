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

package org.apache.asterix.optimizer.cost;

public class Cost implements ICost {

    public static final double MAX_CARD = 1.0e200;
    protected static final int COST_GT = 1;
    protected static final int COST_LT = -1;
    protected static final int COST_EQ = 0;

    private final double cost;

    public Cost() {
        this.cost = 0.0;
    }

    public Cost(double cost) {
        this.cost = cost;
    }

    @Override
    public Cost zeroCost() {
        return new Cost();
    }

    @Override
    public Cost maxCost() {
        return new Cost(MAX_CARD);
    }

    @Override
    public Cost costAdd(ICost iCost2) {
        return new Cost(this.computeTotalCost() + iCost2.computeTotalCost());
    }

    @Override
    public Cost costAdd(ICost iCost2, ICost iCost3) {
        return this.costAdd(iCost2.costAdd(iCost3));
    }

    @Override
    public int costCompare(ICost iCost2) {
        if (this.computeTotalCost() > iCost2.computeTotalCost()) {
            return COST_GT;
        } else if (this.computeTotalCost() < iCost2.computeTotalCost()) {
            return COST_LT;
        } else {
            return COST_EQ;
        }
    }

    @Override
    public boolean costEQ(ICost iCost2) {
        return this.costCompare(iCost2) == COST_EQ;
    }

    @Override
    public boolean costLT(ICost iCost2) {
        return this.costCompare(iCost2) == COST_LT;
    }

    @Override
    public boolean costGT(ICost iCost2) {
        return this.costCompare(iCost2) == COST_GT;
    }

    @Override
    public boolean costLE(ICost iCost2) {
        return this.costLT(iCost2) || this.costEQ(iCost2);
    }

    @Override
    public boolean costGE(ICost iCost2) {
        return this.costGT(iCost2) || this.costEQ(iCost2);
    }

    @Override
    public double computeTotalCost() {
        return this.cost;
    }
}
