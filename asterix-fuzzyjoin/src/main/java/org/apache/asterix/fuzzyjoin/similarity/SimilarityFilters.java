/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin.similarity;

import java.io.Serializable;

public interface SimilarityFilters extends Serializable {
    public int getLengthLowerBound(int length);

    public int getLengthUpperBound(int length);

    public int getPrefixLength(int length);

    public boolean passLengthFilter(int lengthX, int lengthY);

    public boolean passPositionFilter(int noGramsCommon, int positionX, int lengthX, int positionY, int lengthY);

    public float passSimilarityFilter(final int[] tokensX, int startX, int lengthX, final int prefixLengthX,
            final int[] tokensY, int startY, int lengthY, final int prefixLengthY, final int intersectionSizePrefix);

    public float passSimilarityFilter(final int[] tokensX, final int prefixLengthX, final int[] tokensY,
            final int prefixLengthY, final int intersectionSizePrefix);

    public boolean passSuffixFilter(int[] tokensX, int startX, int lengthX, int positionX, int[] tokensY, int startY,
            int lengthY, int positionY);

    public boolean passSuffixFilter(int[] tokensX, int positionX, int[] tokensY, int positionY);
}
