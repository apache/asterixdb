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

package edu.uci.ics.pregelix.example.maximalclique;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import edu.uci.ics.pregelix.api.graph.Edge;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.io.VertexWriter;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat;
import edu.uci.ics.pregelix.api.io.text.TextVertexOutputFormat.TextVertexWriter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.data.VLongNormalizedKeyComputer;
import edu.uci.ics.pregelix.example.io.VLongWritable;
import edu.uci.ics.pregelix.example.trianglecounting.TriangleCountingVertex;

/**
 * The maximal clique example -- find maximal cliques in an undirected graph.
 * The result cliques contains vertexes ordered by the vertex id ascendingly.
 * The algorithm takes advantage of that property to do effective pruning.
 */
public class MaximalCliqueVertex extends Vertex<VLongWritable, CliquesWritable, NullWritable, AdjacencyListWritable> {

    private Map<VLongWritable, AdjacencyListWritable> map = new TreeMap<VLongWritable, AdjacencyListWritable>();
    private List<VLongWritable> vertexList = new ArrayList<VLongWritable>();
    private Map<VLongWritable, Integer> invertedMap = new TreeMap<VLongWritable, Integer>();
    private int largestCliqueSizeSoFar = 0;
    private List<BitSet> currentMaximalCliques = new ArrayList<BitSet>();
    private CliquesWritable tmpValue = new CliquesWritable();
    private List<VLongWritable> cliqueList = new ArrayList<VLongWritable>();

    /**
     * Update the current maximal cliques
     * 
     * @param values
     *            the received adjcency lists
     */
    private void updateCurrentMaximalCliques(Iterator<AdjacencyListWritable> values) {
        map.clear();
        vertexList.clear();
        invertedMap.clear();
        currentMaximalCliques.clear();
        tmpValue.reset();

        // build the initial sub graph
        while (values.hasNext()) {
            AdjacencyListWritable adj = values.next();
            map.put(adj.getSource(), adj);
        }

        // build the vertex list (vertex id in ascending order) and the inverted list of vertexes
        int i = 0;
        for (VLongWritable v : map.keySet()) {
            vertexList.add(v);
            invertedMap.put(v, i++);
        }

        // clean up adjacency list --- remove vertexes who are not neighbors of
        // key
        for (AdjacencyListWritable adj : map.values()) {
            adj.cleanNonMatch(vertexList);
        }

        // get the h-index of the subgraph --- which is the maximum depth to
        // explore
        int[] neighborCounts = new int[map.size()];
        i = 0;
        for (AdjacencyListWritable adj : map.values()) {
            neighborCounts[i++] = adj.numberOfNeighbors();
        }
        Arrays.sort(neighborCounts);
        int h = 0;
        for (i = neighborCounts.length - 1; i >= 0; i--) {
            if (h >= neighborCounts[i]) {
                break;
            }
            h++;
        }

        // the clique size is upper-bounded by h+1
        if (h + 1 < largestCliqueSizeSoFar) {
            return;
        }

        // start depth-first search
        BitSet cliqueSoFar = new BitSet(h);
        for (VLongWritable v : vertexList) {
            cliqueSoFar.set(invertedMap.get(v));
            searchClique(h, cliqueSoFar, 1, v);
            cliqueSoFar.clear();
        }

        // output local maximal cliques
        tmpValue.setSrcId(getVertexId());
        for (BitSet clique : currentMaximalCliques) {
            generateClique(clique);
            tmpValue.addCliques(cliqueList);
            tmpValue.setCliqueSize(clique.cardinality());
        }

        // update the vertex state
        setVertexValue(tmpValue);
    }

    /**
     * Output a clique with vertex ids.
     * 
     * @param clique
     *            the bitmap representation of a clique
     */
    private List<VLongWritable> generateClique(BitSet clique) {
        cliqueList.clear();
        for (int j = 0; j < clique.length();) {
            j = clique.nextSetBit(j);
            VLongWritable v = vertexList.get(j);
            cliqueList.add(v);
            j++;
        }
        return cliqueList;
    }

    /**
     * find cliques using the depth-first search
     * 
     * @param maxDepth
     *            the maximum search depth
     * @param cliqueSoFar
     *            the the cliques found so far
     * @param depthSoFar
     *            the current search depth
     * @param currentSource
     *            the vertex to be added into the clique
     */
    private void searchClique(int maxDepth, BitSet cliqueSoFar, int depthSoFar, VLongWritable currentSource) {
        if (depthSoFar > maxDepth) {
            // update maximal clique info
            updateMaximalClique(cliqueSoFar);
            return;
        }

        AdjacencyListWritable adj = map.get(currentSource);
        Iterator<VLongWritable> neighbors = adj.getNeighbors();
        ++depthSoFar;
        while (neighbors.hasNext()) {
            VLongWritable neighbor = neighbors.next();
            if (!isTested(neighbor, cliqueSoFar) && isClique(neighbor, cliqueSoFar)) {
                // snapshot the clique
                int cliqueLength = cliqueSoFar.length();
                // expand the clique
                cliqueSoFar.set(invertedMap.get(neighbor));
                searchClique(maxDepth, cliqueSoFar, depthSoFar, neighbor);
                // back to the snapshot clique
                cliqueSoFar.set(cliqueLength, cliqueSoFar.length(), false);
            }
        }

        // update maximal clique info
        updateMaximalClique(cliqueSoFar);
    }

    /**
     * Update the maximal clique to a larger one if it exists
     * 
     * @param cliqueSoFar
     *            the clique so far, in the bitmap representation
     */
    private void updateMaximalClique(BitSet cliqueSoFar) {
        int cliqueSize = cliqueSoFar.cardinality();
        if (cliqueSize > largestCliqueSizeSoFar) {
            currentMaximalCliques.clear();
            currentMaximalCliques.add((BitSet) cliqueSoFar.clone());
            largestCliqueSizeSoFar = cliqueSize;
        } else if (cliqueSize == largestCliqueSizeSoFar) {
            currentMaximalCliques.add((BitSet) cliqueSoFar.clone());
        } else {
            return;
        }
    }

    /**
     * Should we test the vertex newVertex?
     * 
     * @param newVertex
     *            the vertex to be tested
     * @param cliqueSoFar
     *            the current clique, in the bitmap representation
     * @return true if new vertex has been tested
     */
    private boolean isTested(VLongWritable newVertex, BitSet cliqueSoFar) {
        int index = invertedMap.get(newVertex);
        int largestSetIndex = cliqueSoFar.length() - 1;
        if (index > largestSetIndex) {
            // we only return cliques with vertexes in the ascending order
            // hence, the new vertex must be larger than the largesetSetIndex in
            // the clique
            return false;
        } else {
            // otherwise, we think the vertex is "tested"
            return true;
        }
    }

    /**
     * Will adding the newVertex yield a bigger clique?
     * 
     * @param newVertex
     *            the new vertex id
     * @param cliqueSoFar
     *            the bitmap representation of the clique
     * @return true if adding the new vertex yelds a bigger clique
     */
    private boolean isClique(VLongWritable newVertex, BitSet cliqueSoFar) {
        AdjacencyListWritable adj = map.get(newVertex);
        // check whether each existing vertex is in the neighbor set of
        // newVertex
        for (int i = 0; i < cliqueSoFar.length();) {
            i = cliqueSoFar.nextSetBit(i);
            VLongWritable v = vertexList.get(i);
            if (!adj.isNeighbor(v)) {
                return false;
            }
            i++;
        }
        return true;
    }

    /**
     * For superstep 1, send outgoing mesages. For superstep 2, calculate
     * maximal cliques. otherwise, vote to halt.
     */
    @Override
    public void compute(Iterator<AdjacencyListWritable> msgIterator) {
        if (getSuperstep() == 1) {
            sortEdges();
            sendOutgoingMsgs(getEdges());
        } else if (getSuperstep() == 2) {
            updateCurrentMaximalCliques(msgIterator);
        } else {
            voteToHalt();
        }
    }

    @Override
    public String toString() {
        return getVertexId() + " " + getVertexValue();
    }

    private static CliquesWritable readMaximalCliqueResult(Configuration conf) {
        try {
            CliquesWritable result = (CliquesWritable) IterationUtils.readGlobalAggregateValue(conf,
                    BspUtils.getJobId(conf));
            return result;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(TriangleCountingVertex.class.getSimpleName());
        job.setVertexClass(MaximalCliqueVertex.class);
        job.setGlobalAggregatorClass(MaximalCliqueAggregator.class);
        job.setDynamicVertexValueSize(true);
        job.setVertexInputFormatClass(TextMaximalCliqueInputFormat.class);
        job.setVertexOutputFormatClass(MaximalCliqueVertexOutputFormat.class);
        job.setNoramlizedKeyComputerClass(VLongNormalizedKeyComputer.class);
        Client.run(args, job);
        System.out.println("maximal cliques: \n" + readMaximalCliqueResult(job.getConfiguration()));
    }

    /**
     * Send the adjacency lists
     * 
     * @param edges
     *            the outgoing edges
     */
    private void sendOutgoingMsgs(List<Edge<VLongWritable, NullWritable>> edges) {
        for (int i = 0; i < edges.size(); i++) {
            if (edges.get(i).getDestVertexId().get() < getVertexId().get()) {
                // only add emit for the vertexes whose id is smaller than the
                // vertex id
                // to avoid the duplicate removal step,
                // because all the resulting cliques will have vertexes in the
                // ascending order.
                AdjacencyListWritable msg = new AdjacencyListWritable();
                msg.setSource(getVertexId());
                for (int j = i + 1; j < edges.size(); j++) {
                    msg.addNeighbor(edges.get(j).getDestVertexId());
                }
                sendMsg(edges.get(i).getDestVertexId(), msg);
            }
        }
    }

    /**
     * Maximal Clique VertexWriter
     */
    public static class MaximalCliqueVertexWriter extends
            TextVertexWriter<VLongWritable, CliquesWritable, NullWritable> {
        public MaximalCliqueVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(Vertex<VLongWritable, CliquesWritable, NullWritable, ?> vertex) throws IOException,
                InterruptedException {
            getRecordWriter().write(new Text(vertex.getVertexId().toString()),
                    new Text(vertex.getVertexValue().toString()));
        }
    }

    /**
     * output format for maximal clique
     */
    public static class MaximalCliqueVertexOutputFormat extends
            TextVertexOutputFormat<VLongWritable, CliquesWritable, NullWritable> {

        @Override
        public VertexWriter<VLongWritable, CliquesWritable, NullWritable> createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
            return new MaximalCliqueVertexWriter(recordWriter);
        }

    }
}
