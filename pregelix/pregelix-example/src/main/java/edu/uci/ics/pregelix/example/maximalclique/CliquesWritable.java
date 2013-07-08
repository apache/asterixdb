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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * The representation of cliques stored in a vertex.
 */
public class CliquesWritable implements Writable {

    private List<VLongWritable> cliques = new ArrayList<VLongWritable>();
    private int sizeOfClique = 0;
    private VLongWritable srcId = new VLongWritable(0);

    public CliquesWritable(List<VLongWritable> cliques, int sizeOfClique) {
        this.cliques = cliques;
        this.sizeOfClique = sizeOfClique;
    }

    public CliquesWritable() {

    }

    /**
     * Set the srcId
     * 
     * @param srcId
     *            the source vertex Id
     */
    public void setSrcId(VLongWritable srcId) {
        this.srcId = srcId;
    }

    /**
     * Set the size of cliques.
     * 
     * @param sizeOfClique
     *            the size of each maximal clique
     */
    public void setCliqueSize(int sizeOfClique) {
        this.sizeOfClique = sizeOfClique;
    }

    /**
     * Add the clique vertexes
     * 
     * @param cliques
     *            the list of vertexes -- can contain multiple cliques
     */
    public void addCliques(CliquesWritable cliques) {
        this.cliques.addAll(cliques.cliques);
    }

    /**
     * Add the clique vertexes
     * 
     * @param cliques
     *            the list of vertexes -- can contain multiple cliques
     */
    public void addCliques(List<VLongWritable> vertexes) {
        this.cliques.addAll(vertexes);
    }

    /**
     * @return the size of the clique
     */
    public int getSizeOfClique() {
        return sizeOfClique;
    }

    /**
     * rese the clique
     */
    public void reset() {
        this.cliques.clear();
        this.sizeOfClique = 0;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        cliques.clear();
        int numCliques = input.readInt();
        if (numCliques < 0) {
            sizeOfClique = 0;
            return;
        }
        sizeOfClique = input.readInt();
        for (int i = 0; i < numCliques; i++) {
            for (int j = 0; j < sizeOfClique; j++) {
                VLongWritable vid = new VLongWritable();
                vid.readFields(input);
                cliques.add(vid);
            }
        }

        if (srcId == null) {
            srcId = new VLongWritable();
        }
        srcId.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        if (sizeOfClique <= 0) {
            output.writeInt(-1);
            return;
        }
        output.writeInt(cliques.size() / sizeOfClique);
        output.writeInt(sizeOfClique);

        for (int i = 0; i < cliques.size(); i++) {
            cliques.get(i).write(output);
        }

        srcId.write(output);
    }

    @Override
    public String toString() {
        if (sizeOfClique == 0)
            return "";
        StringBuffer sb = new StringBuffer();
        int numCliques = cliques.size() / sizeOfClique;
        for (int i = 0; i < numCliques; i++) {
            sb.append(srcId);
            sb.append(",");
            int start = i * sizeOfClique;
            for (int j = 0; j < sizeOfClique - 1; j++) {
                sb.append(cliques.get(start + j));
                sb.append(",");
            }
            sb.append(cliques.get(start + sizeOfClique - 1));
            sb.append(";");
        }
        return sb.toString();
    }
}
