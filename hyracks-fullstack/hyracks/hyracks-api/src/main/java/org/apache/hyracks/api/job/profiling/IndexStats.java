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
package org.apache.hyracks.api.job.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.api.com.job.profiling.counters.Counter;
import org.apache.hyracks.api.io.IWritable;
import org.apache.hyracks.api.job.profiling.counters.ICounter;

/**
 * Currently, this class represents the stats of an index across all the partitions. The stats are not per partition.
 */
public class IndexStats implements IWritable, Serializable {

    private static final long serialVersionUID = 1L;

    private final ICounter numPages;
    private String indexName;

    public IndexStats(String indexName, long numPages) {
        this.indexName = indexName;
        this.numPages = new Counter("numPages");
        this.numPages.set(numPages);
    }

    public static IndexStats create(DataInput input) throws IOException {
        String indexName = input.readUTF();
        long numPages = input.readLong();
        return new IndexStats(indexName, numPages);
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeUTF(indexName);
        output.writeLong(numPages.get());
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        indexName = input.readUTF();
        numPages.set(input.readLong());
    }

    public void updateNumPages(long delta) {
        numPages.update(delta);
    }

    public long getNumPages() {
        return numPages.get();
    }

    @Override
    public String toString() {
        return "IndexStats{indexName='" + indexName + "', numPages=" + numPages.get() + '}';
    }
}
