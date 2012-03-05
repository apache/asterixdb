/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.feed.mgmt;

import java.io.Serializable;

public class FeedId implements Serializable {

    private final String dataverse;
    private final String dataset;

    public FeedId(String dataverse, String dataset) {
        this.dataset = dataset;
        this.dataverse = dataverse;
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
        return dataverse.hashCode() + dataset.hashCode();
    }

}
