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

    // Minimum cardinality for operators is 2.1 to prevent bad plans due to cardinality under estimation errors.
    public static final double MIN_CARD = 2.1;
    private static final int COST_EQ = 0;

    private final double cost;

    public Cost() {
        this.cost = 0.0;
    }

    public Cost(double cost) {
        this.cost = cost;
    }

    @Override
    public ICost zeroCost() {
        return new Cost();
    }

    @Override
    public ICost maxCost() {
        return new Cost(MAX_CARD);
    }

    @Override
    public ICost costAdd(ICost cost) {
        return new Cost(computeTotalCost() + cost.computeTotalCost());
    }

    @Override
    public boolean costEQ(ICost cost) {
        return compareTo(cost) == COST_EQ;
    }

    @Override
    public boolean costLT(ICost cost) {
        return compareTo(cost) < COST_EQ;
    }

    @Override
    public boolean costGT(ICost cost) {
        return compareTo(cost) > COST_EQ;
    }

    @Override
    public boolean costLE(ICost cost) {
        return compareTo(cost) <= COST_EQ;
    }

    @Override
    public boolean costGE(ICost cost) {
        return compareTo(cost) >= COST_EQ;
    }

    @Override
    public double computeTotalCost() {
        return cost;
    }

    @Override
    public int compareTo(ICost cost) {
        return Double.compare(computeTotalCost(), cost.computeTotalCost());
    }

    @Override
    public String toString() {
        return Double.toString(cost);
    }
}
