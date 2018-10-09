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

import java.io.Serializable;

public interface SimilarityFilters extends Serializable {
    int getLengthLowerBound(int length);

    int getLengthUpperBound(int length);

    int getPrefixLength(int length);

    boolean passLengthFilter(int lengthX, int lengthY);

    boolean passPositionFilter(int noGramsCommon, int positionX, int lengthX, int positionY, int lengthY);

    float passSimilarityFilter(final int[] tokensX, int startX, int lengthX, final int prefixLengthX,
            final int[] tokensY, int startY, int lengthY, final int prefixLengthY, final int intersectionSizePrefix);

    float passSimilarityFilter(final int[] tokensX, final int prefixLengthX, final int[] tokensY,
            final int prefixLengthY, final int intersectionSizePrefix);

    boolean passSuffixFilter(int[] tokensX, int startX, int lengthX, int positionX, int[] tokensY, int startY,
            int lengthY, int positionY);

    boolean passSuffixFilter(int[] tokensX, int positionX, int[] tokensY, int positionY);
}
