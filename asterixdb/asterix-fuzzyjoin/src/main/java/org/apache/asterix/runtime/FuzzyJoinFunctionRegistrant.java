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
package org.apache.asterix.runtime;

import org.apache.asterix.om.functions.IFunctionCollection;
import org.apache.asterix.om.functions.IFunctionRegistrant;
import org.apache.asterix.runtime.evaluators.functions.CountHashedGramTokensDescriptor;
import org.apache.asterix.runtime.evaluators.functions.CountHashedWordTokensDescriptor;
import org.apache.asterix.runtime.evaluators.functions.EditDistanceCheckDescriptor;
import org.apache.asterix.runtime.evaluators.functions.EditDistanceContainsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.EditDistanceDescriptor;
import org.apache.asterix.runtime.evaluators.functions.EditDistanceListIsFilterableDescriptor;
import org.apache.asterix.runtime.evaluators.functions.EditDistanceStringIsFilterableDescriptor;
import org.apache.asterix.runtime.evaluators.functions.GramTokensDescriptor;
import org.apache.asterix.runtime.evaluators.functions.HashedGramTokensDescriptor;
import org.apache.asterix.runtime.evaluators.functions.HashedWordTokensDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PrefixLenJaccardDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SimilarityJaccardCheckDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SimilarityJaccardDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SimilarityJaccardPrefixCheckDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SimilarityJaccardPrefixDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SimilarityJaccardSortedCheckDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SimilarityJaccardSortedDescriptor;
import org.apache.asterix.runtime.evaluators.functions.SpatialIntersectDescriptor;
import org.apache.asterix.runtime.evaluators.functions.WordTokensDescriptor;

public class FuzzyJoinFunctionRegistrant implements IFunctionRegistrant {
    @Override
    public void register(IFunctionCollection fc) {
        // TODO: decide how should we deal these two weird functions as
        // the number of arguments of the function depend on the first few arguments.
        fc.add(SimilarityJaccardPrefixDescriptor.FACTORY);
        fc.add(SimilarityJaccardPrefixCheckDescriptor.FACTORY);

        // Spatial
        fc.addGenerated(SpatialIntersectDescriptor.FACTORY);

        // fuzzyjoin function
        fc.addGenerated(PrefixLenJaccardDescriptor.FACTORY);
        fc.addGenerated(WordTokensDescriptor.FACTORY);
        fc.addGenerated(HashedWordTokensDescriptor.FACTORY);
        fc.addGenerated(CountHashedWordTokensDescriptor.FACTORY);
        fc.addGenerated(GramTokensDescriptor.FACTORY);
        fc.addGenerated(HashedGramTokensDescriptor.FACTORY);
        fc.addGenerated(CountHashedGramTokensDescriptor.FACTORY);
        fc.addGenerated(EditDistanceDescriptor.FACTORY);
        fc.addGenerated(EditDistanceCheckDescriptor.FACTORY);
        fc.addGenerated(EditDistanceStringIsFilterableDescriptor.FACTORY);
        fc.addGenerated(EditDistanceListIsFilterableDescriptor.FACTORY);
        fc.addGenerated(EditDistanceContainsDescriptor.FACTORY);
        fc.addGenerated(SimilarityJaccardDescriptor.FACTORY);
        fc.addGenerated(SimilarityJaccardCheckDescriptor.FACTORY);
        fc.addGenerated(SimilarityJaccardSortedDescriptor.FACTORY);
        fc.addGenerated(SimilarityJaccardSortedCheckDescriptor.FACTORY);
    }
}
