/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.evaluators.common;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetricJaccard;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class SimilarityJaccardSortedCheckEvaluator extends SimilarityJaccardCheckEvaluator {

    protected final SimilarityMetricJaccard jaccard = new SimilarityMetricJaccard();
    
    public SimilarityJaccardSortedCheckEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
            throws AlgebricksException {
        super(args, output);
    }

    @Override
    protected float computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
            throws AlgebricksException {
        return jaccard.getSimilarity(firstListIter, secondListIter, jaccThresh);
    }
}
