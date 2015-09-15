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

package org.apache.asterix.fuzzyjoin.similarity;

import org.apache.asterix.fuzzyjoin.tokenizer.Tokenizer;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class SimilarityMetric {

    public static int getIntersectSize(IListIterator tokensX, IListIterator tokensY) throws HyracksDataException {
        int intersectSize = 0;
        while (tokensX.hasNext() && tokensY.hasNext()) {
            int cmp = tokensX.compare(tokensY);
            if (cmp > 0) {
                tokensY.next();
            } else if (cmp < 0) {
                tokensX.next();
            } else {
                intersectSize++;
                tokensX.next();
                tokensY.next();
            }
        }
        return intersectSize;
    }

    public static int getIntersectSize(int[] tokensX, int startX, int lengthX, int[] tokensY, int startY, int lengthY) {
        int posX = 0;
        int posY = 0;
        int intersectSize = 0;

        while (posX < lengthX && posY < lengthY) {
            int tokenX = tokensX[startX + posX];
            int tokenY = tokensY[startY + posY];
            if (tokenX > tokenY) {
                posY++;
            } else if (tokenX < tokenY) {
                posX++;
            } else {
                intersectSize++;
                posX++;
                posY++;
            }
        }

        return intersectSize;
    }

    public static int getIntersectSize(int[] tokensX, int startX, int[] tokensY, int startY) {
        // int intersectSize = 0;
        //
        // while (startX < tokensX.length && startY < tokensY.length) {
        // int tokenX = tokensX[startX];
        // int tokenY = tokensY[startY];
        // if (tokenX > tokenY) {
        // startY++;
        // } else if (tokenX < tokenY) {
        // startX++;
        // } else {
        // intersectSize++;
        // startX++;
        // startY++;
        // }
        // }
        //
        // return intersectSize;
        return getIntersectSize(tokensX, startX, tokensX.length, tokensY, startY, tokensY.length);
    }

    public static int getIntersectSize(int[] tokensX, int[] tokensY) {
        return getIntersectSize(tokensX, 0, tokensX.length, tokensY, 0, tokensY.length);
    }

    public static PartialIntersect getPartialIntersectSize(int[] tokensX, int startX, int lengthX, int[] tokensY,
            int startY, int lengthY, int tokenStop) {
        PartialIntersect parInter = new PartialIntersect();
        getPartialIntersectSize(tokensX, startX, lengthX, tokensY, startY, lengthY, tokenStop, parInter);
        return parInter;
    }

    public static void getPartialIntersectSize(int[] tokensX, int startX, int lengthX, int[] tokensY, int startY,
            int lengthY, int tokenStop, PartialIntersect parInter) {
        int posX = 0;
        int posY = 0;
        int intersectSize = 0;

        parInter.reset();
        while (posX < lengthX && posY < lengthY) {
            int tokenX = tokensX[startX + posX];
            int tokenY = tokensY[startY + posY];
            if (tokenX > tokenY) {
                posY++;
            } else if (tokenX < tokenY) {
                posX++;
            } else {
                intersectSize++;
                if (!parInter.isSet()) {
                    parInter.posXStart = posX;
                    parInter.posYStart = posY;
                    parInter.set();
                }
                if (tokenX == tokenStop) {
                    parInter.posXStop = posX;
                    parInter.posYStop = posY;
                    parInter.intersectSize = intersectSize;
                }
                posX++;
                posY++;
            }
        }
    }

    public static PartialIntersect getPartialIntersectSize(int[] tokensX, int[] tokensY, int tokenStop) {
        return getPartialIntersectSize(tokensX, 0, tokensX.length, tokensY, 0, tokensY.length, tokenStop);
    }

    // @SuppressWarnings("unchecked")
    // public static int getIntersectSize(DataBag tokensX, DataBag tokensY) {
    // int intersectSize = 0;
    //
    // Iterator<Tuple> iteratorX = tokensX.iterator();
    // Iterator<Tuple> iteratorY = tokensY.iterator();
    //
    // Tuple nextX = null;
    // Tuple nextY = null;
    //
    // while ((nextX != null || iteratorX.hasNext())
    // && (nextY != null || iteratorY.hasNext())) {
    // if (nextX == null) {
    // nextX = iteratorX.next();
    // }
    // if (nextY == null) {
    // nextY = iteratorY.next();
    // }
    //
    // int cmp = nextX.compareTo(nextY);
    // if (cmp > 0) {
    // nextY = null;
    // } else if (cmp < 0) {
    // nextX = null;
    // } else {
    // intersectSize++;
    // nextX = null;
    // nextY = null;
    // }
    // }
    //
    // return intersectSize;
    // }

    // public abstract float getSimilarity(DataBag tokensX, DataBag tokensY);

    // public abstract float getSimilarity(DataBag tokensX, int lengthX,
    // DataBag tokensY, int lengthY);

    public float getSimilarity(IListIterator tokensX, IListIterator tokensY) throws HyracksDataException {
        int intersectionSize = SimilarityMetric.getIntersectSize(tokensX, tokensY);
        int totalSize = tokensX.size() + tokensY.size();

        return (float) intersectionSize / (totalSize - intersectionSize);
    }

    public abstract float getSimilarity(int[] tokensX, int startX, int lengthX, int[] tokensY, int startY, int lengthY);

    public abstract float getSimilarity(int[] tokensX, int[] tokensY);

    public abstract float getSimilarity(String stringX, String stringY, Tokenizer tokenizer);
}
