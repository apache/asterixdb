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
package edu.uci.ics.asterix.external.feed.lifecycle;

import java.io.Serializable;

/**
 * A unique identifier for a feed (dataset).
 */
public class FeedId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String dataverse;
    private final String dataset;
    private final int hashcode;

    public FeedId(String dataverse, String dataset) {
        this.dataset = dataset;
        this.dataverse = dataverse;
        this.hashcode = (dataverse + "." + dataset).hashCode();
    }

    public String getDataverse() {
        return dataverse;
    }

    public String getDataset() {
        return dataset;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof FeedId)) {
            return false;
        }
        if (((FeedId) o).getDataset().equals(dataset) && ((FeedId) o).getDataverse().equals(dataverse)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public String toString() {
        return dataverse + "." + dataset;
    }

}
