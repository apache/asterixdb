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

package edu.uci.ics.pregelix.benchmark.vertex;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.RandomWalkVertex;
import org.apache.giraph.utils.MathUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * The PageRank algorithm, with uniform transition probabilities on the edges
 * http://en.wikipedia.org/wiki/PageRank
 */
public class PageRankVertex extends RandomWalkVertex<NullWritable> {

    @Override
    protected double transitionProbability(double stateProbability, Edge<LongWritable, NullWritable> edge) {
        return stateProbability / getNumEdges();
    }

    @Override
    protected double recompute(Iterable<DoubleWritable> partialRanks, double teleportationProbability) {

        // rank contribution from incident neighbors
        double rankFromNeighbors = MathUtils.sum(partialRanks);
        // rank contribution from dangling vertices
        double danglingContribution = getDanglingProbability() / getTotalNumVertices();

        // recompute rank
        return (1d - teleportationProbability) * (rankFromNeighbors + danglingContribution) + teleportationProbability
                / getTotalNumVertices();
    }
}
