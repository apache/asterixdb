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
package org.apache.asterix.fuzzyjoin.similarity;

import java.util.Arrays;

public class SimilarityFiltersJaccard implements SimilarityFilters {
    class Partition {
        public int startL;
        public int lengthL;
        public int startR;
        public int lengthR;
        public int hamming;

        public Partition(int startL, int lengthL, int startR, int lengthR, int hamming) {
            this.startL = startL;
            this.lengthL = lengthL;
            this.startR = startR;
            this.lengthR = lengthR;
            this.hamming = hamming;
        }
    }

    private static final long serialVersionUID = 1L;

    private static final int MAX_DEPTH = 2;

    // Note here that, this may be unsafe when the string length is larger than 100.
    public static final double EPSILON = 0.000001;

    public static int getLengthLowerBound(int length, double simThr) {
        return safeCeilingDouble(simThr * length);
    }

    /* Length filter derived from Jaccard(lengthX, lengthY) >= simThr
     * 1. lengthX >= lengthY * simThr, or equivalently, lengthY <= 1 / simThr * lengthX
     * 2. lengthY >= lengthX * simThr
     * */
    public static boolean passLengthFilter(int lengthX, int lengthY, double simThr) {
        return getLengthLowerBound(lengthX, simThr) <= lengthY
                && (lengthY < 1 / simThr * lengthX || Math.abs(lengthY - 1 / simThr * lengthX) < EPSILON);
    }

    protected double simThr;

    public SimilarityFiltersJaccard(float similarityThreshold) {
        reset(similarityThreshold);
    }

    public int getIndexPrefixLength(int length) {
        return length - safeCeilingDouble(2 * simThr / (1 + simThr) * length) + 1;
    }

    private static int safeCeilingDouble(double d) {
        if (Math.abs(d - Math.floor(d)) < EPSILON) {
            return (int) Math.floor(d);
        } else {
            return (int) Math.ceil(d);
        }
    }

    public int getIntersectLowerBound(int lengthX, int lengthY) {
        return safeCeilingDouble(simThr * (lengthX + lengthY) / (1 + simThr));
    }

    public int getIntersectUpperBound(int noGramsCommon, int positionX, int positionY, int lengthX, int lengthY) {
        return noGramsCommon + Math.min(lengthX - positionX - 1, lengthY - positionY - 1);
    }

    @Override
    public int getLengthLowerBound(int length) {
        return getLengthLowerBound(length, simThr);
    }

    @Override
    public int getLengthUpperBound(int length) {
        return (int) Math.floor(1 / simThr * length);
    }

    private Partition getPartition(int[] tokens, int start, int length, int w, int posL, int posR) {
        int p;
        if (tokens[posL] > w) {
            p = posL;
        } else if (tokens[posR] < w) {
            p = posR + 1;
        } else {
            p = Arrays.binarySearch(tokens, start, start + length, w);
        }

        if (p < 0) {
            p = -p - 1;
        }

        if (p >= start && p < start + length && tokens[p] == w) {
            return new Partition(start, p - start, p + 1, start + length - p - 1, 0);
        }
        return new Partition(start, p - start, p, start + length - p, 1);
    }

    @Override
    public int getPrefixLength(int length) {
        if (length == 0) {
            return 0;
        }
        return length - safeCeilingDouble(simThr * length) + 1;
    }

    public double getSimilarityThreshold() {
        return simThr;
    }

    private int getSuffixFilter(int[] tokensX, int startX, int lengthX, int[] tokensY, int startY, int lengthY,
            int hammingMax, int depth) {
        final int lengthDiff = Math.abs(lengthX - lengthY);

        if (depth > MAX_DEPTH || lengthX == 0 || lengthY == 0) {
            return lengthDiff;
        }

        final int mid = startY + lengthY / 2 + lengthY % 2 - 1;
        final int offset = (hammingMax - lengthDiff) / 2;

        int offsetL;
        int offsetR;
        if (lengthX < lengthY) {
            offsetL = 1;
            offsetR = 0;
        } else {
            offsetL = 0;
            offsetR = 1;
        }
        Partition partitionY = new Partition(startY, mid - startY, mid + 1, startY + lengthY - mid - 1, 0);

        Partition partitionX = getPartition(tokensX, startX, lengthX, tokensY[mid],
                Math.max(mid + startX - startY - offset - lengthDiff * offsetL, startX),
                Math.min(mid + startX - startY + offset + lengthDiff * offsetR, startX + lengthX - 1));

        int hammingPart = partitionX.hamming;

        int hamming = Math.abs(partitionX.lengthL - partitionY.lengthL)
                + Math.abs(partitionX.lengthR - partitionY.lengthR) + hammingPart;

        if (hamming <= hammingMax) {
            int hammingL = getSuffixFilter(tokensX, partitionX.startL, partitionX.lengthL, tokensY, partitionY.startL,
                    partitionY.lengthL, hammingMax - Math.abs(partitionX.lengthR - partitionY.lengthR) - hammingPart,
                    depth + 1);
            hamming = hammingL + Math.abs(partitionX.lengthR - partitionY.lengthR) + hammingPart;

            if (hamming <= hammingMax) {
                int hammingR = getSuffixFilter(tokensX, partitionX.startR, partitionX.lengthR, tokensY,
                        partitionY.startR, partitionY.lengthR, hammingMax - hammingL - hammingPart, depth + 1);
                hamming = hammingL + hammingR + hammingPart;
            }
        }
        return hamming;
    }

    @Override
    public boolean passLengthFilter(int lengthX, int lengthY) {
        return passLengthFilter(lengthX, lengthY, simThr);
    }

    /**
     * @param noGramsCommon number of grams in common
     * @param positionX     position of the last gram in common on X
     * @param positionY     position of the last gram in common on X
     * @param lengthX       total length of X
     * @param lengthY       total length of Y
     * @return
     */
    @Override
    public boolean passPositionFilter(int noGramsCommon, int positionX, int lengthX, int positionY, int lengthY) {
        return getIntersectUpperBound(noGramsCommon, positionX, positionY, lengthX,
                lengthY) >= getIntersectLowerBound(lengthX, lengthY);
    }

    @Override
    public float passSimilarityFilter(final int[] tokensX, int startX, int lengthX, final int prefixLengthX,
            final int[] tokensY, int startY, int lengthY, final int prefixLengthY, final int intersectionSizePrefix) {
        final int length = lengthX;
        final int token = tokensX[startX + Math.min(prefixLengthX, lengthX) - 1];
        final int lengthProbe = lengthY;
        final int tokenProbe = tokensY[startY + prefixLengthY - 1];

        final int intersectSizeLowerBound = getIntersectLowerBound(length, lengthProbe);
        int intersectSize = 0;

        if (token < tokenProbe) {
            if (intersectionSizePrefix + length - prefixLengthX >= intersectSizeLowerBound) {
                intersectSize = intersectionSizePrefix
                        + SimilarityMetric.getIntersectSize(tokensX, startX + prefixLengthX, lengthX - prefixLengthX,
                                tokensY, startY + intersectionSizePrefix, lengthY - intersectionSizePrefix);
            }
        } else if (token > tokenProbe) {
            if (intersectionSizePrefix + lengthProbe - prefixLengthY >= intersectSizeLowerBound) {
                intersectSize = intersectionSizePrefix + SimilarityMetric.getIntersectSize(tokensX,
                        startX + intersectionSizePrefix, lengthX - intersectionSizePrefix, tokensY,
                        startY + prefixLengthY, lengthY - prefixLengthY);
            }
        } else {
            if (intersectionSizePrefix + lengthProbe - prefixLengthY >= intersectSizeLowerBound) {
                intersectSize =
                        intersectionSizePrefix + SimilarityMetric.getIntersectSize(tokensX, startX + prefixLengthX,
                                lengthX - prefixLengthX, tokensY, startY + prefixLengthY, lengthY - prefixLengthY);
            }
        }

        if (intersectSize >= intersectSizeLowerBound) {
            return ((float) intersectSize) / (length + lengthProbe - intersectSize);
        }
        return 0;
    }

    /**
     * @param tokensX                ordered list of the tokens in X
     * @param prefixLengthX          prefix length of x derived from prefix filter based on simThr
     * @param tokensY                ordered list of the tokens in Y
     * @param prefixLengthY          prefix length of Y derived from prefix filter based on simThr
     * @param intersectionSizePrefix
     * @return similarity if it is above or equal to the similarity threshold, 0 otherwise
     */
    @Override
    public float passSimilarityFilter(final int[] tokensX, final int prefixLengthX, final int[] tokensY,
            final int prefixLengthY, final int intersectionSizePrefix) {
        return passSimilarityFilter(tokensX, 0, tokensX.length, prefixLengthX, tokensY, 0, tokensY.length,
                prefixLengthY, intersectionSizePrefix);
    }

    @Override
    public boolean passSuffixFilter(int[] tokensX, int tokensStartX, int tokensLengthX, int positionX, int[] tokensY,
            int tokensStartY, int tokensLengthY, int positionY) {
        int hammingMax = tokensLengthX + tokensLengthY
                - 2 * safeCeilingDouble(simThr / (1 + simThr) * (tokensLengthX + tokensLengthY))
                - (positionX + 1 + positionY + 1 - 2);
        int hamming = getSuffixFilter(tokensX, tokensStartX + positionX + 1, tokensLengthX - positionX - 1, tokensY,
                tokensStartY + positionY + 1, tokensLengthY - positionY - 1, hammingMax, 1);
        return hamming <= hammingMax;
    }

    @Override
    public boolean passSuffixFilter(int[] tokensX, int positionX, int[] tokensY, int positionY) {
        return passSuffixFilter(tokensX, 0, tokensX.length, positionX, tokensY, 0, tokensY.length, positionY);
    }

    public void reset(float similarityThreshold) {
        simThr = similarityThreshold;
    }
}
