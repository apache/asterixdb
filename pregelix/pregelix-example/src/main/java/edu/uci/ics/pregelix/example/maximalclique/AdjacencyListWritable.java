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
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import edu.uci.ics.pregelix.api.io.WritableSizable;
import edu.uci.ics.pregelix.example.io.VLongWritable;

/**
 * The adjacency list contains <src, list-of-neighbors>
 */
public class AdjacencyListWritable implements WritableSizable {

    private VLongWritable sourceVertex = new VLongWritable();
    private Set<VLongWritable> destinationVertexes = new TreeSet<VLongWritable>();

    public AdjacencyListWritable() {
    }

    public void reset() {
        this.destinationVertexes.clear();
    }

    public void setSource(VLongWritable source) {
        this.sourceVertex = source;
    }

    public void addNeighbor(VLongWritable neighbor) {
        destinationVertexes.add(neighbor);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        sourceVertex = new VLongWritable();
        destinationVertexes.clear();
        sourceVertex.readFields(input);
        int numberOfNeighbors = input.readInt();
        for (int i = 0; i < numberOfNeighbors; i++) {
            VLongWritable neighbor = new VLongWritable();
            neighbor.readFields(input);
            destinationVertexes.add(neighbor);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        sourceVertex.write(output);
        output.writeInt(destinationVertexes.size());
        for (VLongWritable dest : destinationVertexes) {
            dest.write(output);
        }
    }

    public int numberOfNeighbors() {
        return destinationVertexes.size();
    }

    public void removeNeighbor(VLongWritable v) {
        destinationVertexes.remove(v);
    }

    public VLongWritable getSource() {
        return sourceVertex;
    }

    public Iterator<VLongWritable> getNeighbors() {
        return destinationVertexes.iterator();
    }

    public void cleanNonMatch(Collection<VLongWritable> matches) {
        destinationVertexes.retainAll(matches);
    }

    public boolean isNeighbor(VLongWritable v) {
        return destinationVertexes.contains(v);
    }

    @Override
    public int sizeInBytes() {
        int size = 4; // the size of list bytes
        for (VLongWritable dest : destinationVertexes) {
            size += dest.sizeInBytes();
        }
        return size;
    }

}
