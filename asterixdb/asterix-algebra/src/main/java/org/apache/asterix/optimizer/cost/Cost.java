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

    private static final int SEQ_IO_WEIGHT = 1;
    private static final int RAND_IO_WEIGHT = 0;
    // not using random costs any more because index access happens after sorts. May consider removing this component in V2

    private final double docsProcessed;
    private final double docsProduced;
    private final double docsSent;
    private final double overFlow;
    private final double ioSeq;
    private final double ioRand;

    public Cost() {
        this(0, 0, 0, 0, 0, 0);
    }

    public Cost(double docsProcessed, double docsProduced, double docsSent, double overFlow, double ioSeq,
            double ioRand) {
        this.docsProcessed = docsProcessed;
        this.docsProduced = docsProduced;
        this.docsSent = docsSent;
        this.overFlow = overFlow;
        this.ioSeq = ioSeq;
        this.ioRand = ioRand;
    }

    @Override
    public ICost zeroCost() {
        return new Cost();
    }

    @Override
    public ICost maxCost() {
        return new Cost(MAX_CARD, MAX_CARD, MAX_CARD, MAX_CARD, 0, 0);
    }

    @Override
    public ICost costAdd(ICost cost) {
        Cost other = (Cost) cost;
        return new Cost(docsProcessed + other.docsProcessed, docsProduced + other.docsProduced,
                docsSent + other.docsSent, overFlow + other.overFlow, ioSeq + other.ioSeq, ioRand + other.ioRand);
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
        return docsProcessed + docsProduced + docsSent + overFlow + ioSeq * SEQ_IO_WEIGHT + ioRand * RAND_IO_WEIGHT;
    }

    @Override
    public int compareTo(ICost cost) {
        return Double.compare(computeTotalCost(), cost.computeTotalCost());
    }

    public double getDocsProcessed() {
        return docsProcessed;
    }

    public double getDocsProduced() {
        return docsProduced;
    }

    public double getDocsSent() {
        return docsSent;
    }

    public double getOverFlow() {
        return overFlow;
    }

    public double getIoSeq() {
        return ioSeq;
    }

    public double getIoRand() {
        return ioRand;
    }

    @Override
    public String toString() {
        return "{docsProcessed = " + docsProcessed + ", docsProduced = " + docsProduced + ", docsSent = " + docsSent
                + ", overFlow = " + overFlow + ", ioSeq = " + ioSeq + ", ioRand = " + ioRand + '}';
    }
}
