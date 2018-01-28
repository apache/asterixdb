/**
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

package org.apache.asterix.fuzzyjoin;

public class FuzzyFiltersJaccard {

    /**
     * type is double because with float .8 / (1 + .8) * (8 + 10) = 8.0...01
     */
    protected final double simThr;
    protected final double simThrExpr;

    public FuzzyFiltersJaccard(double similarityThreshold) {
        simThr = similarityThreshold;
        simThrExpr = simThr / (1 + simThr);
    }

    public int getIndexPrefixLength(int length) {
        return length - (int) Math.ceil(2 * simThrExpr * length) + 1;
    }

    public int getIntersectLowerBound(int lengthX, int lengthY) {
        return (int) Math.ceil(simThrExpr * (lengthX + lengthY));
    }

    public long getIntersectLowerBound(long lengthX, long lengthY) {
        return (long) Math.ceil(simThrExpr * (lengthX + lengthY));
    }

    public int getIntersectUpperBound(int noGramsCommon, int positionX, int positionY, int lengthX, int lengthY) {
        return noGramsCommon + Math.min(lengthX - positionX - 1, lengthY - positionY - 1);
    }

    public long getIntersectUpperBound(int noGramsCommon, long positionX, long positionY, long lengthX, long lengthY) {
        return noGramsCommon + Math.min(lengthX - positionX - 1, lengthY - positionY - 1);
    }

    public int getLengthLowerBound(int length) {
        return (int) Math.ceil(simThr * length);
    }

    public long getLengthLowerBound(long length) {
        return (long) Math.ceil(simThr * length);
    }

    public int getPrefixLength(int length) {
        return length - (int) Math.ceil(simThr * length) + 1;
    }

    public long getPrefixLength(long length) {
        return length - (long) Math.ceil(simThr * length) + 1;
    }

    public double getSimilarityThreshold() {
        return simThr;
    }

    public boolean passLengthFilter(int lengthX, int lengthY) {
        return getLengthLowerBound(lengthX) <= lengthY && lengthY <= 1 / simThr * lengthX;
    }

    public boolean passLengthFilter(long lengthX, long lengthY) {
        return getLengthLowerBound(lengthX) <= lengthY && lengthY <= 1 / simThr * lengthX;
    }

    /**
     * @param noGramsCommon
     *            number of grams in common
     * @param positionX
     *            position of the last gram in common on X
     * @param positionY
     *            position of the last gram in common on X
     * @param lengthX
     * @param lengthY
     * @return
     */
    public boolean passPositionFilter(int noGramsCommon, int positionX, int positionY, int lengthX, int lengthY) {
        return getIntersectUpperBound(noGramsCommon, positionX, positionY, lengthX,
                lengthY) >= getIntersectLowerBound(lengthX, lengthY);
    }

    public boolean passPositionFilter(int noGramsCommon, long positionX, long positionY, long lengthX, long lengthY) {
        return getIntersectUpperBound(noGramsCommon, positionX, positionY, lengthX,
                lengthY) >= getIntersectLowerBound(lengthX, lengthY);
    }

}
